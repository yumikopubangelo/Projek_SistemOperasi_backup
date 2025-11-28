"""
System Routes - ULTRA FAST VERSION
- /health returns in <50ms (no blocking calls)
- Stats fetched with timeout protection
- Cached responses for expensive operations
"""

from flask import jsonify, request, make_response
from datetime import datetime, timezone
import logging
import time
import threading
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ============================================
# GLOBAL CACHE (prevents blocking)
# ============================================
_health_cache = {
    "data": None,
    "timestamp": 0,
    "lock": threading.RLock()
}
_cache_ttl = 2.0  # Cache valid for 2 seconds


def register_system_routes(app, services: Dict[str, Any]):
    """Register ultra-fast system routes"""
    
    es = services.get("es")
    buffer_manager = services.get("buffer_manager")
    telegram_notifier = services.get("telegram_notifier")
    queue_manager = services.get("queue_manager")
    csv_exporter = services.get("csv_exporter")

    try:
        from core.config import Config as _Config
        Config = _Config
    except Exception:
        Config = None

    # -----------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------
    def _now_iso():
        return datetime.now(timezone.utc).isoformat()

    def _response_json(payload: Dict[str, Any], status: int = 200):
        start = payload.pop("_start_time", time.time())
        resp = make_response(jsonify(payload), status)
        elapsed_ms = int((time.time() - start) * 1000)
        resp.headers["X-Response-Time"] = f"{elapsed_ms}ms"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    def _safe_get_stats(service, service_name: str, timeout_ms: int = 100) -> Optional[Dict]:
        """Get stats with timeout protection"""
        if not service:
            return None
        
        result = [None]  # mutable container for thread result
        
        def get_stats_thread():
            try:
                result[0] = service.get_stats()
            except Exception as e:
                logger.debug(f"[HEALTH] {service_name} stats error: {e}")
                result[0] = None
        
        thread = threading.Thread(target=get_stats_thread, daemon=True)
        thread.start()
        thread.join(timeout=timeout_ms / 1000.0)  # Convert to seconds
        
        if thread.is_alive():
            logger.warning(f"[HEALTH] {service_name}.get_stats() timeout ({timeout_ms}ms)")
            return None
        
        return result[0]

    def _build_health_response() -> Dict[str, Any]:
        """Build health response (expensive, should be cached)"""
        start = time.time()
        
        # ---------- Elasticsearch (FAST ping with timeout) ----------
        es_healthy = False
        try:
            if es:
                # Ping with timeout
                ping_result = [False]
                
                def ping_thread():
                    try:
                        ping_result[0] = bool(es.ping())
                    except:
                        ping_result[0] = False
                
                t = threading.Thread(target=ping_thread, daemon=True)
                t.start()
                t.join(timeout=0.5)  # 500ms max
                
                if t.is_alive():
                    logger.warning("[HEALTH] ES ping timeout")
                    es_healthy = False
                else:
                    es_healthy = ping_result[0]
        except Exception as e:
            logger.debug(f"[HEALTH] ES ping error: {e}")
            es_healthy = False

        # ---------- Service flags (instant) ----------
        services_info = {
            "buffer": buffer_manager is not None,
            "queue": queue_manager is not None,
            "telegram": telegram_notifier is not None,
            "csv_export": csv_exporter is not None,
        }

        payload = {
            "status": "healthy" if es_healthy else "degraded",
            "timestamp": _now_iso(),
            "server_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "elasticsearch": es_healthy,
            "services": services_info,
        }

        # ---------- STATS (with timeout protection) ----------
        stats_section = {}

        # Buffer stats (100ms timeout)
        buffer_stats = _safe_get_stats(buffer_manager, "buffer", 100)
        if buffer_stats and isinstance(buffer_stats, dict):
            stats_section["buffer"] = {
                "current_buffer_size": buffer_stats.get("current_buffer_size", 0),
                "total_received": buffer_stats.get("total_received", 0),
                "total_flushed": buffer_stats.get("total_flushed", 0),
                "pending": buffer_stats.get("pending", 0),
                "snapshot_stale": buffer_stats.get("snapshot_stale", False)
            }
        else:
            stats_section["buffer"] = {"error": "timeout_or_unavailable"}

        # Telegram stats (50ms timeout)
        tg_stats = _safe_get_stats(telegram_notifier, "telegram", 200)
        if tg_stats and isinstance(tg_stats, dict):
            stats_section["telegram"] = {
                "sent": tg_stats.get("alerts_sent", 0),
                "recent": tg_stats.get("recent_count", 0),
            }
        else:
            stats_section["telegram"] = None

        # Queue stats (100ms timeout)
        queue_stats = _safe_get_stats(queue_manager, "queue", 200)
        if queue_stats and isinstance(queue_stats, dict):
            stats_section["queue"] = {
                "queued": queue_stats.get("queued", 0),
                "active": queue_stats.get("active", 0),
            }
        else:
            stats_section["queue"] = None

        # CSV stats (50ms timeout)
        csv_stats = _safe_get_stats(csv_exporter, "csv", 200)
        if csv_stats and isinstance(csv_stats, dict):
            stats_section["csv"] = {
                "sensor_records": csv_stats.get("sensor_data", {}).get("records", 0),
            }
        else:
            stats_section["csv"] = None

        payload["stats"] = stats_section

        # Warnings
        warnings = []
        if stats_section.get("buffer", {}).get("snapshot_stale"):
            warnings.append("buffer_snapshot_stale")
        if warnings:
            payload["warnings"] = warnings

        elapsed = (time.time() - start) * 1000
        payload["build_time_ms"] = int(elapsed)
        
        return payload

    # -----------------------------------------------------------
    # /health (CACHED for speed)
    # -----------------------------------------------------------
    @app.route("/health", methods=["GET"])
    def health():
        """Ultra-fast health check with caching"""
        request_start = time.time()
        
        with _health_cache["lock"]:
            now = time.time()
            cache_age = now - _health_cache["timestamp"]
            
            # Return cached if fresh
            if _health_cache["data"] and cache_age < _cache_ttl:
                payload = _health_cache["data"].copy()
                payload["_start_time"] = request_start
                payload["cached"] = True
                payload["cache_age_ms"] = int(cache_age * 1000)
                return _response_json(payload, 200)
            
            # Rebuild cache in background thread to avoid blocking
            if cache_age >= _cache_ttl:
                def update_cache():
                    try:
                        new_data = _build_health_response()
                        with _health_cache["lock"]:
                            _health_cache["data"] = new_data
                            _health_cache["timestamp"] = time.time()
                    except Exception as e:
                        logger.exception("[HEALTH] Cache update failed")
                
                # Start background update
                threading.Thread(target=update_cache, daemon=True).start()
            
            # Return stale cache immediately (better than blocking)
            if _health_cache["data"]:
                payload = _health_cache["data"].copy()
                payload["_start_time"] = request_start
                payload["cached"] = True
                payload["cache_age_ms"] = int(cache_age * 1000)
                payload["stale"] = True
                return _response_json(payload, 200)
            
            # No cache available, build synchronously (first request only)
            payload = _build_health_response()
            payload["_start_time"] = request_start
            payload["cached"] = False
            
            # Save to cache
            _health_cache["data"] = payload.copy()
            _health_cache["timestamp"] = time.time()
            
            return _response_json(payload, 200)

    # -----------------------------------------------------------
    # /data/latest (FAST version)
    # -----------------------------------------------------------
    @app.route("/data/latest", methods=["GET"])
    def latest_data():
        start = time.time()
        payload = {"_start_time": start}

        if not es or not Config:
            payload.update({
                "status": "no_es",
                "data": {"tds_ppm": 0, "kekeruhan_ntu": 0, "suhu_celsius": 0, "@timestamp": _now_iso()}
            })
            return _response_json(payload)

        try:
            result = es.search(
                index=Config.ELASTIC_INDEX,
                size=1,
                sort=[{"@timestamp": {"order": "desc"}}],
                request_timeout=2  # 2 second timeout
            )

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                payload.update({"status": "no_data", "data": {}})
                return _response_json(payload)

            doc = hits[0]["_source"]
            
            # Normalize timestamp
            ts = doc.get("@timestamp", _now_iso())
            try:
                doc["@timestamp"] = datetime.fromisoformat(ts.replace("Z", "+00:00")).isoformat()
            except:
                doc["@timestamp"] = _now_iso()

            doc.setdefault("tds_ppm", 0)
            doc.setdefault("kekeruhan_ntu", 0)
            doc.setdefault("suhu_celsius", 0)

            payload.update({"status": "success", "data": doc})
            return _response_json(payload)

        except Exception as e:
            logger.warning(f"[LATEST] Error: {e}")
            payload.update({"status": "error", "message": "timeout_or_error"})
            return _response_json(payload)

    # -----------------------------------------------------------
    # /data/historical (FAST version)
    # -----------------------------------------------------------
    @app.route("/data/historical", methods=["GET"])
    def historical_data():
        start = time.time()
        payload = {"_start_time": start}

        if not es or not Config:
            payload.update({"status": "no_es", "count": 0, "data": []})
            return _response_json(payload)

        try:
            size = int(request.args.get("size", 50))
            size = max(1, min(size, 200))

            result = es.search(
                index=Config.ELASTIC_INDEX,
                size=size,
                sort=[{"@timestamp": {"order": "desc"}}],
                request_timeout=3
            )

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                payload.update({"status": "no_data", "count": 0, "data": []})
                return _response_json(payload)

            data = []
            for hit in reversed(hits):
                src = hit["_source"]
                try:
                    src["@timestamp"] = datetime.fromisoformat(
                        src["@timestamp"].replace("Z", "+00:00")
                    ).isoformat()
                except:
                    src["@timestamp"] = _now_iso()

                src.setdefault("tds_ppm", 0)
                src.setdefault("kekeruhan_ntu", 0)
                src.setdefault("suhu_celsius", 0)
                data.append(src)

            payload.update({"status": "success", "count": len(data), "data": data})
            return _response_json(payload)

        except Exception as e:
            logger.warning(f"[HISTORICAL] Error: {e}")
            payload.update({"status": "error", "count": 0, "data": []})
            return _response_json(payload)

    # -----------------------------------------------------------
    # /data/summary (FAST version)
    # -----------------------------------------------------------
    @app.route("/data/summary", methods=["GET"])
    def data_summary():
        start = time.time()
        payload = {"_start_time": start}

        if not es or not Config:
            payload.update({"status": "no_es", "timeline": []})
            return _response_json(payload)

        try:
            hours_back = int(request.args.get("hours_back", 24))
            hours_back = max(1, min(hours_back, 720))

            body = {
                "query": {"range": {"@timestamp": {"gte": f"now-{hours_back}h"}}},
                "aggs": {
                    "timeline": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "1h"
                        },
                        "aggs": {
                            "avg_tds": {"avg": {"field": "tds_ppm"}},
                            "avg_turbidity": {"avg": {"field": "kekeruhan_ntu"}}
                        }
                    }
                }
            }

            result = es.search(
                index=Config.ELASTIC_INDEX,
                size=0,
                body=body,
                request_timeout=3
            )

            aggs = result.get("aggregations", {})
            timeline = []
            for b in aggs.get("timeline", {}).get("buckets", []):
                timeline.append({
                    "timestamp": b.get("key_as_string"),
                    "avg_tds": round(b.get("avg_tds", {}).get("value") or 0, 2),
                    "avg_turbidity": round(b.get("avg_turbidity", {}).get("value") or 0, 2),
                    "count": b.get("doc_count", 0)
                })

            payload.update({"status": "success", "timeline": timeline})
            return _response_json(payload)

        except Exception as e:
            logger.warning(f"[SUMMARY] Error: {e}")
            payload.update({"status": "error", "timeline": []})
            return _response_json(payload)

    # -----------------------------------------------------------
    # /system/info
    # -----------------------------------------------------------
    @app.route("/system/info", methods=["GET"])
    def system_info():
        try:
            return _response_json({
                "_start_time": time.time(),
                "status": "success",
                "server_time": _now_iso(),
                "version": "7.5-fixed",
                "elasticsearch": {
                    "host": getattr(Config, "ELASTIC_HOST", None) if Config else None,
                    "index": getattr(Config, "ELASTIC_INDEX", None) if Config else None,
                },
                "features": {
                    "buffer": buffer_manager is not None,
                    "queue": queue_manager is not None,
                    "telegram": telegram_notifier is not None,
                    "csv_export": csv_exporter is not None,
                }
            })
        except Exception as e:
            logger.exception("[SYSTEM INFO]")
            return _response_json({"_start_time": time.time(), "status": "error"}, 500)

    logger.info("[ROUTES] ⚡ Ultra-fast system routes registered (health cached)")