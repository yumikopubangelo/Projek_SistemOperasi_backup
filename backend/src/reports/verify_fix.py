"""
System Routes (Stats removed, merged into /health)
- /stats is fully removed
- /health now includes fast non-blocking stats
- Designed for dashboard stability & low latency
"""

from flask import jsonify, request, make_response
from datetime import datetime, timezone
import logging
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)


def register_system_routes(app, services: Dict[str, Any]):
    """
    Register all system-related routes.

    Services dict keys (optional):
        - es
        - buffer_manager
        - telegram_notifier
        - queue_manager
        - csv_exporter
        - ml_service
    """

    es = services.get("es")
    buffer_manager = services.get("buffer_manager")
    telegram_notifier = services.get("telegram_notifier")
    queue_manager = services.get("queue_manager")
    csv_exporter = services.get("csv_exporter")

    # Try import config safely
    try:
        from core.config import Config as _Config
        Config = _Config
    except Exception:
        Config = None
        logger.debug("[SYSTEM ROUTES] Config not available")

    # -----------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------
    def _now_iso():
        return datetime.now(timezone.utc).isoformat()

    def _response_json(payload: Dict[str, Any], status: int = 200):
        start = payload.pop("_start_time", None)
        if start is None:
            start = time.time()

        resp = make_response(jsonify(payload), status)
        resp.headers["X-Response-Time"] = f"{int((time.time() - start) * 1000)}ms"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    # -----------------------------------------------------------
    # /health   → includes system stats
    # -----------------------------------------------------------
    @app.route("/health", methods=["GET"])
    def health():
        start = time.time()
        payload = {"_start_time": start}

        # ---------- Elasticsearch health ----------
        es_healthy = False
        try:
            if es:
                es_healthy = bool(es.ping())
        except Exception:
            es_healthy = False

        # ---------- Service flags ----------
        services_info = {
            "buffer": buffer_manager is not None,
            "queue": queue_manager is not None,
            "telegram": telegram_notifier is not None,
            "csv_export": csv_exporter is not None,
        }

        payload.update({
            "status": "healthy" if es_healthy else "degraded",
            "timestamp": _now_iso(),
            "server_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "elasticsearch": es_healthy,
            "services": services_info,
            "note": "health+stats"
        })

        # ---------- STATS Section (FAST / NON-BLOCKING) ----------
        stats_section = {
            "buffer": None,
            "telegram": None,
            "queue": None,
            "csv": None
        }

        # Buffer stats (cached, non-blocking)
        try:
            if buffer_manager:
                b = buffer_manager.get_stats()
                if isinstance(b, dict):
                    stats_section["buffer"] = {
                        "current_buffer_size": b.get("current_buffer_size"),
                        "configured_buffer_size": b.get("configured_buffer_size"),
                        "total_received": b.get("total_received"),
                        "total_flushed": b.get("total_flushed"),
                        "pending": b.get("pending"),
                        "traffic_rpm": b.get("traffic_rpm"),
                        "avg_flush_time": b.get("avg_flush_time"),
                        "last_flush": b.get("last_flush"),
                        "snapshot_stale": b.get("snapshot_stale", False)
                    }
        except Exception as e:
            logger.warning("[HEALTH] buffer stats error: %s", e)
            stats_section["buffer"] = {"error": str(e)}

        # Telegram stats
        try:
            if telegram_notifier:
                tg = telegram_notifier.get_stats()
                if isinstance(tg, dict):
                    stats_section["telegram"] = {
                        "sent": tg.get("alerts_sent", 0),
                        "failed": tg.get("alerts_failed", 0),
                        "recent": tg.get("recent_count", 0),
                        "last_alert": tg.get("last_alert")
                    }
        except Exception as e:
            logger.warning("[HEALTH] telegram stats error: %s", e)
            stats_section["telegram"] = {"error": str(e)}

        # Queue stats
        try:
            if queue_manager:
                q = queue_manager.get_stats()
                if isinstance(q, dict):
                    stats_section["queue"] = {
                        "queued": q.get("queued", q.get("queued_tasks", 0)),
                        "active": q.get("active", 0),
                        "completed": q.get("total_completed", 0),
                        "failed": q.get("total_failed", 0),
                        "workers": q.get("workers", 0)
                    }
        except Exception as e:
            logger.warning("[HEALTH] queue stats error: %s", e)
            stats_section["queue"] = {"error": str(e)}

        # CSV Exporter stats
        try:
            if csv_exporter:
                c = csv_exporter.get_stats()
                if isinstance(c, dict):
                    stats_section["csv"] = {
                        "sensor_records": c.get("sensor_data", {}).get("records", 0),
                        "anomaly_records": c.get("anomaly_log", {}).get("records", 0)
                    }
        except Exception as e:
            logger.warning("[HEALTH] csv stats error: %s", e)
            stats_section["csv"] = {"error": str(e)}

        payload["stats"] = stats_section

        # Add warnings
        warnings = []
        if stats_section.get("buffer", {}).get("snapshot_stale"):
            warnings.append("buffer_snapshot_stale")
        if warnings:
            payload["warnings"] = warnings

        return _response_json(payload, 200)

    # -----------------------------------------------------------
    # /data/latest
    # -----------------------------------------------------------
    @app.route("/data/latest", methods=["GET"])
    def latest_data():
        start = time.time()
        payload = {"_start_time": start}

        try:
            if not es or not Config:
                payload.update({
                    "status": "no_es",
                    "message": "Elasticsearch not configured",
                    "data": {
                        "tds_ppm": 0,
                        "kekeruhan_ntu": 0,
                        "suhu_celsius": 0,
                        "@timestamp": _now_iso()
                    }
                })
                return _response_json(payload)

            try:
                index = getattr(Config, "ELASTIC_INDEX", None)
                result = es.search(
                    index=index,
                    size=1,
                    sort=[{"@timestamp": {"order": "desc"}}],
                    request_timeout=3
                )
            except Exception as e:
                logger.warning("[LATEST] ES error: %s", e)
                payload.update({"status": "error", "message": "elastic_error"})
                return _response_json(payload)

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                payload.update({"status": "no_data"})
                return _response_json(payload)

            doc = hits[0]["_source"]
            ts = doc.get("@timestamp", _now_iso())

            try:
                doc["@timestamp"] = datetime.fromisoformat(ts.replace("Z", "+00:00")).isoformat()
            except:
                doc["@timestamp"] = _now_iso()

            doc.setdefault("tds_ppm", 0)
            doc.setdefault("kekeruhan_ntu", 0)
            doc.setdefault("suhu_celsius", 0)

            payload.update({
                "status": "success",
                "data": doc,
                "retrieved_at": _now_iso()
            })
            return _response_json(payload)

        except Exception as e:
            logger.exception("[LATEST] Unexpected")
            payload.update({"status": "error", "message": str(e)})
            return _response_json(payload)

    # -----------------------------------------------------------
    # /data/historical
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
                request_timeout=5
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

            payload.update({
                "status": "success",
                "count": len(data),
                "data": data,
                "retrieved_at": _now_iso()
            })
            return _response_json(payload)

        except Exception as e:
            logger.exception("[HISTORICAL] error")
            payload.update({"status": "error", "count": 0, "message": str(e), "data": []})
            return _response_json(payload)

    # -----------------------------------------------------------
    # /data/summary
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
                    "tds": {"stats": {"field": "tds_ppm"}},
                    "turbidity": {"stats": {"field": "kekeruhan_ntu"}},
                    "temperature": {"stats": {"field": "suhu_celsius"}},
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
                request_timeout=5
            )

            aggs = result.get("aggregations", {})
            total = result.get("hits", {}).get("total", {}).get("value", 0)

            timeline = []
            for b in aggs.get("timeline", {}).get("buckets", []):
                timeline.append({
                    "timestamp": b.get("key_as_string"),
                    "avg_tds": round(b.get("avg_tds", {}).get("value") or 0, 2),
                    "avg_turbidity": round(b.get("avg_turbidity", {}).get("value") or 0, 2),
                    "count": b.get("doc_count", 0)
                })

            payload.update({
                "status": "success",
                "documents": total,
                "timeline": timeline,
                "retrieved_at": _now_iso()
            })
            return _response_json(payload)

        except Exception as e:
            logger.exception("[SUMMARY] error")
            payload.update({"status": "error", "timeline": [], "message": str(e)})
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
                "server_time_local": datetime.now().isoformat(),
                "version": "7.5",
                "elasticsearch": {
                    "host": getattr(Config, "ELASTIC_HOST", None) if Config else None,
                    "index": getattr(Config, "ELASTIC_INDEX", None) if Config else None,
                },
                "features": {
                    "buffer": buffer_manager is not None,
                    "queue": queue_manager is not None,
                    "telegram": telegram_notifier is not None,
                    "csv_export": csv_exporter is not None,
                    "ml_service": services.get("ml_service") is not None,
                }
            })
        except Exception as e:
            logger.exception("[SYSTEM INFO]")
            return _response_json({"_start_time": time.time(), "status": "error", "message": str(e)}, 500)

    logger.info("[ROUTES] System routes registered (health includes stats)")
