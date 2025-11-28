"""
Queue Service Integration for AquaGuard - Enhanced Version (REVISED)

- Backwards-compatible constructor for AquaGuardQueueManager (accepts use_rq and **kwargs)
- Stats refresher uses AquaGuardQueueManager.get_stats() (safer)
- Defensive coding + clearer logging
- Routes registration unchanged in behaviour but safer around missing services
"""

import logging
import time
from threading import Lock, Thread
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from flask import jsonify, request

# local import
from .queue_service import EnhancedQueueService, TaskPriority as ModuleTaskPriority

logger = logging.getLogger(__name__)

# ---------------------------
# Stats cache for fast /stats
# ---------------------------
_stats_cache: Dict[str, Any] = {"value": None, "last_updated": None}
_stats_lock = Lock()
_stats_refresh_interval = 2.0  # seconds
_stats_refresher_started_attr = "_queue_stats_refresher_started"


def _refresh_stats_background(queue_manager):
    """Background thread that periodically refreshes queue stats."""
    logger.info("[STATS] Stats refresher thread started")
    while True:
        try:
            stats = None
            try:
                # call queue_manager.get_stats() (defensive)
                if queue_manager is None:
                    stats = None
                else:
                    stats = queue_manager.get_stats()
            except Exception:
                logger.debug("[STATS] queue_manager.get_stats() failed when refreshing cache", exc_info=True)
                stats = None

            with _stats_lock:
                _stats_cache["value"] = stats
                _stats_cache["last_updated"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            logger.exception("[STATS] Unexpected error in stats refresher")
        time.sleep(_stats_refresh_interval)


def _ensure_stats_refresher_started(app, queue_manager):
    """Start background refresher once per app process."""
    if not getattr(app, _stats_refresher_started_attr, False):
        t = Thread(target=_refresh_stats_background, args=(queue_manager,), daemon=True)
        t.start()
        setattr(app, _stats_refresher_started_attr, True)
        logger.info("[STATS] Stats refresher thread initialized")


# ---------------------------
# Timestamp normalization helpers
# ---------------------------
def _safe_int(value) -> Optional[int]:
    """Attempt to coerce value to int; return None on failure."""
    try:
        if isinstance(value, float):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            s = value.strip()
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                return int(s)
        return None
    except Exception:
        return None


def _ms_epoch_to_iso(ms_value) -> Optional[str]:
    """
    Convert epoch milliseconds (or seconds) to ISO8601 UTC string with Z suffix.
    """
    if ms_value is None or (isinstance(ms_value, str) and not ms_value.strip()):
        return None

    if isinstance(ms_value, str):
        s = ms_value.strip()
        if ("-" in s and ":" in s) or s.endswith("Z"):
            try:
                if s.endswith("Z"):
                    s2 = s[:-1]
                    dt = datetime.fromisoformat(s2)
                else:
                    dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass

    maybe_int = _safe_int(ms_value)
    if maybe_int is None:
        try:
            maybe_int = int(float(ms_value))
        except Exception:
            return None

    ms = maybe_int
    try:
        # Accept both seconds and milliseconds; heuristics:
        if ms > 1_000_000_000_000:
            dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        else:
            # treat as seconds (including ms values < 1e12 still likely seconds in some contexts)
            # If caller uses milliseconds but value small, fallback will still try and likely be wrong;
            # preserve best-effort behavior from previous code.
            dt = datetime.fromtimestamp(ms, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _normalize_anomaly_timestamps(result: Dict[str, Any]) -> None:
    """Convert known timestamp fields in anomaly items to ISO strings in-place."""
    if not isinstance(result, dict):
        return
    anomalies = result.get("anomalies")
    if not anomalies or not isinstance(anomalies, list):
        return

    for a in anomalies:
        if not isinstance(a, dict):
            continue
        for key in ("timestamp", "@timestamp", "ts", "time", "event_time"):
            if key in a:
                iso = _ms_epoch_to_iso(a.get(key))
                if iso:
                    a[key] = iso
                if "timestamp_iso" not in a:
                    a["timestamp_iso"] = iso
                break


# ===========================
# AquaGuardQueueManager class
# ===========================
class AquaGuardQueueManager:
    """
    Centralized Queue Manager for AquaGuard async operations.

    Backwards-compatible: accepts `use_rq` and arbitrary **kwargs so older/newer
    initialize_services() calls won't crash if they pass extra params.
    """

    def __init__(
        self,
        es_client,
        data_ingestion=None,
        telegram_notifier=None,
        ml_service=None,
        csv_exporter=None,
        buffer_manager=None,
        queue: Optional[EnhancedQueueService] = None,
        use_rq: Optional[bool] = None,
        **kwargs
    ):
        """Initialize with dependency injection"""
        self.es_client = es_client
        self.data_ingestion = data_ingestion
        self.telegram_notifier = telegram_notifier
        self.ml_service = ml_service
        self.csv_exporter = csv_exporter
        self.buffer_manager = buffer_manager

        # Create or use provided queue
        if queue is None:
            # Try to pass use_rq if EnhancedQueueService supports it; fallback otherwise
            try:
                if use_rq is None:
                    # call without use_rq first (most common)
                    self.queue = EnhancedQueueService(
                        max_workers=5,
                        max_queue_size=200,
                        health_check_interval=30,
                        circuit_breaker_threshold=5,
                        circuit_breaker_timeout=60
                    )
                else:
                    # attempt to pass use_rq keyword (newer EnhancedQueueService may accept it)
                    self.queue = EnhancedQueueService(
                        max_workers=5,
                        max_queue_size=200,
                        health_check_interval=30,
                        circuit_breaker_threshold=5,
                        circuit_breaker_timeout=60,
                        use_rq=use_rq
                    )
            except TypeError:
                # fallback: call without use_rq if signature mismatch
                try:
                    logger.debug("[QUEUE] EnhancedQueueService doesn't accept use_rq kwarg; retrying without it")
                    self.queue = EnhancedQueueService(
                        max_workers=5,
                        max_queue_size=200,
                        health_check_interval=30,
                        circuit_breaker_threshold=5,
                        circuit_breaker_timeout=60
                    )
                except Exception:
                    logger.exception("[QUEUE] Failed to instantiate EnhancedQueueService (fallback)")
                    self.queue = None
            except Exception:
                logger.exception("[QUEUE] Unexpected error creating EnhancedQueueService")
                self.queue = None
        else:
            self.queue = queue

        # Expose TaskPriority for external use
        self.TaskPriority = ModuleTaskPriority

        logger.info("[AQUAGUARD QUEUE] Manager initialized with integrations; rq=%s", bool(self.queue))

    # ==================== DATA INGESTION TASKS ====================
    def submit_data_ingestion(
        self,
        data: Dict[str, Any],
        priority: ModuleTaskPriority = ModuleTaskPriority.HIGH
    ) -> Optional[str]:
        if not self.data_ingestion:
            logger.error("[QUEUE] Data ingestion service not available")
            return None

        if not self.queue:
            logger.error("[QUEUE] Queue not initialized - cannot submit data ingestion")
            return None

        try:
            # Import task function (not bound method)
            from services.rq_tasks import data_ingestion_ingest

            return self.queue.submit_task(
                name="data_ingestion",
                func=data_ingestion_ingest,
                args=(data,),
                priority=priority,
                timeout=10.0,
                max_retries=3
            )
        except Exception:
            logger.exception("[QUEUE] submit_data_ingestion failed")
            return None

    def submit_batch_ingestion(
        self,
        data_batch: List[Dict[str, Any]],
        priority: ModuleTaskPriority = ModuleTaskPriority.HIGH
    ) -> Optional[str]:
        if not self.data_ingestion:
            logger.error("[QUEUE] Data ingestion service not available")
            return None

        if not self.queue:
            logger.error("[QUEUE] Queue not initialized - cannot submit batch ingestion")
            return None

        try:
            from services.rq_tasks import data_ingestion_ingest_batch

            return self.queue.submit_task(
                name=f"batch_ingest:{len(data_batch)}",
                func=data_ingestion_ingest_batch,
                args=(data_batch,),
                priority=priority,
                timeout=60.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_batch_ingestion failed")
            return None

    # ==================== ML TASKS ====================
    def submit_ml_anomaly_detection(
        self,
        hours_back: int = 24,
        min_score: float = 50.0,
        priority: ModuleTaskPriority = ModuleTaskPriority.MEDIUM
    ) -> Optional[str]:
        if not self.ml_service:
            logger.error("[QUEUE] ML service not available")
            return None

        if not self.queue:
            logger.error("[QUEUE] Queue not initialized - cannot submit ML anomaly detection")
            return None

        try:
            from services.rq_tasks import ml_anomaly_detection

            return self.queue.submit_task(
                name="ml_anomaly_detection",
                func=ml_anomaly_detection,
                kwargs={"hours_back": hours_back, "min_score": min_score},
                priority=priority,
                timeout=300.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_ml_anomaly_detection failed")
            return None

    def submit_ml_status_check(
        self,
        priority: ModuleTaskPriority = ModuleTaskPriority.LOW
    ) -> Optional[str]:
        if not self.ml_service or not self.queue:
            return None
        try:
            from services.rq_tasks import ml_status_check

            return self.queue.submit_task(
                name="ml_status_check",
                func=ml_status_check,
                priority=priority,
                timeout=30.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_ml_status_check failed")
            return None

    def submit_ml_summary(
        self,
        hours_back: int = 24,
        priority: ModuleTaskPriority = ModuleTaskPriority.LOW
    ) -> Optional[str]:
        if not self.ml_service or not self.queue:
            return None
        try:
            from services.rq_tasks import ml_summary

            return self.queue.submit_task(
                name="ml_summary",
                func=ml_summary,
                kwargs={"hours_back": hours_back},
                priority=priority,
                timeout=60.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_ml_summary failed")
            return None

    # ==================== PREDICTION TASKS ====================
    def submit_prediction_task(
        self,
        prediction_type: str,
        params: Dict[str, Any],
        priority: ModuleTaskPriority = ModuleTaskPriority.MEDIUM
    ) -> Optional[str]:
        if not self.queue:
            logger.error("[QUEUE] Queue not initialized - cannot submit prediction")
            return None
        try:
            from services.rq_tasks import prediction_task

            return self.queue.submit_task(
                name=f"prediction:{prediction_type}",
                func=prediction_task,
                kwargs={"prediction_type": prediction_type, "params": params},
                priority=priority,
                timeout=120.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_prediction_task failed")
            return None

    def _run_prediction(self, prediction_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal helper for prediction execution.
        Used by worker tasks.
        """
        try:
            from prediction_engine import WaterQualityPredictor
            predictor = WaterQualityPredictor()

            hours_back = int(params.get("hours_back", 48))
            size = min(max(1, hours_back * 12), 1000)

            qbody = {
                "query": {
                    "range": {
                        "@timestamp": {"gte": f"now-{hours_back}h"}
                    }
                },
                "size": size,
                "sort": [{"@timestamp": {"order": "desc"}}]
            }

            result = self.es_client.search(
                index=getattr(self.es_client, "index_name", None) or None,
                body=qbody,
                size=size
            )

            hits = []
            try:
                hits = result.get("hits", {}).get("hits", [])
            except Exception:
                if isinstance(result, list):
                    hits = result

            if not hits:
                return {"status": "error", "message": "No historical data available"}

            historical_data = [h.get("_source", h) for h in reversed(hits)]

            if prediction_type == "filter_rul":
                return predictor.calculate_filter_rul(historical_data)
            elif prediction_type == "time_to_threshold":
                return predictor.predict_time_to_threshold(
                    historical_data,
                    parameter=params.get("parameter", "tds_ppm"),
                    threshold=params.get("threshold", 700)
                )
            elif prediction_type == "next_value":
                return predictor.predict_next_value(
                    historical_data,
                    parameter=params.get("parameter", "tds_ppm"),
                    hours_ahead=params.get("hours_ahead", 1)
                )
            else:
                return {"status": "error", "message": f"Unknown prediction type: {prediction_type}"}

        except Exception:
            logger.exception("[PREDICTION] Error running prediction")
            raise

    # ==================== CSV EXPORT TASKS ====================
    def submit_csv_export(
        self,
        export_type: str,
        data: Dict[str, Any],
        priority: ModuleTaskPriority = ModuleTaskPriority.LOW
    ) -> Optional[str]:
        if not self.csv_exporter:
            logger.error("[QUEUE] CSV exporter not available")
            return None

        if not self.queue:
            logger.error("[QUEUE] Queue not initialized - cannot submit csv_export")
            return None

        try:
            from services.rq_tasks import csv_export

            return self.queue.submit_task(
                name=f"csv_export:{export_type}",
                func=csv_export,
                kwargs={"export_type": export_type, "data": data},
                priority=priority,
                timeout=60.0,
                max_retries=3
            )
        except Exception:
            logger.exception("[QUEUE] submit_csv_export failed")
            return None

    # ==================== BUFFER MANAGEMENT ====================
    def submit_buffer_flush(
        self,
        priority: ModuleTaskPriority = ModuleTaskPriority.HIGH
    ) -> Optional[str]:
        if not self.buffer_manager or not self.queue:
            logger.error("[QUEUE] Buffer manager or queue not available")
            return None
        try:
            from services.rq_tasks import buffer_flush

            return self.queue.submit_task(
                name="buffer_flush",
                func=buffer_flush,
                priority=priority,
                timeout=30.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_buffer_flush failed")
            return None

    # ==================== BULK PROCESSING ====================
    def submit_bulk_data_processing(
        self,
        data_batch: list,
        priority: ModuleTaskPriority = ModuleTaskPriority.HIGH
    ) -> Optional[str]:
        if not self.queue:
            logger.error("[QUEUE] Queue not initialized - cannot submit bulk processing")
            return None
        try:
            from services.rq_tasks import bulk_data_processing

            return self.queue.submit_task(
                name=f"bulk_processing:{len(data_batch)}",
                func=bulk_data_processing,
                args=(data_batch,),
                priority=priority,
                timeout=300.0,
                max_retries=2
            )
        except Exception:
            logger.exception("[QUEUE] submit_bulk_data_processing failed")
            return None

    def _process_bulk_data(self, data_batch: list) -> Dict[str, Any]:
        """
        Internal helper for bulk data processing.
        Used by worker tasks.
        """
        try:
            if hasattr(self.es_client, "bulk_index"):
                success, failed = self.es_client.bulk_index(data_batch)
            else:
                success = 0
                failed = 0
                for doc in data_batch:
                    try:
                        self.es_client.index(body=doc)
                        success += 1
                    except Exception:
                        failed += 1

            # CSV logging best-effort
            try:
                if self.csv_exporter:
                    for d in data_batch:
                        self.csv_exporter.log_sensor_data(d)
            except Exception:
                logger.exception("[BULK PROCESSING] CSV logging failed (non-fatal)")

            logger.info("[BULK PROCESSING] Completed: %d success, %d failed", success, failed)
            return {
                "status": "success",
                "total": len(data_batch),
                "success": success,
                "failed": failed
            }
        except Exception:
            logger.exception("[BULK PROCESSING] Error")
            raise

    # ==================== TASK MANAGEMENT ====================
    def pause_task(self, task_id: str) -> bool:
        try:
            if not self.queue:
                return False
            if hasattr(self.queue, "pause_task"):
                return bool(self.queue.pause_task(task_id))
            logger.debug("[QUEUE] pause_task not implemented")
            return False
        except Exception:
            logger.exception("[QUEUE] pause_task raised exception")
            return False

    def resume_task(self, task_id: str) -> bool:
        try:
            if not self.queue:
                return False
            if hasattr(self.queue, "resume_task"):
                return bool(self.queue.resume_task(task_id))
            logger.debug("[QUEUE] resume_task not implemented")
            return False
        except Exception:
            logger.exception("[QUEUE] resume_task raised exception")
            return False

    def cancel_task(self, task_id: str) -> bool:
        try:
            if not self.queue:
                return False
            if hasattr(self.queue, "cancel_task"):
                return bool(self.queue.cancel_task(task_id))
            logger.debug("[QUEUE] cancel_task not implemented")
            return False
        except Exception:
            logger.exception("[QUEUE] cancel_task raised exception")
            return False

    def get_stats(self) -> Dict[str, Any]:
        try:
            if not self.queue:
                return {"queued_total": 0, "active_count": 0, "mode": "none"}
            return self.queue.get_stats()
        except Exception:
            logger.exception("[QUEUE] get_stats failed")
            return {"queued_total": None, "active_count": None}

    def get_api_health(self) -> Dict[str, Any]:
        try:
            if not self.queue:
                return {"status": "not_configured", "details": "queue not initialized"}
            if hasattr(self.queue, "get_api_health"):
                return self.queue.get_api_health()
            if hasattr(self.queue, "get_api_health_status"):
                return self.queue.get_api_health_status()
            stats = self.get_stats()
            return {"status": "ok", "stats": stats}
        except Exception as e:
            logger.exception("[QUEUE] get_api_health failed")
            return {"status": "error", "error": str(e)}


# ---------------------------
# Flask routes for queue
# ---------------------------
def register_queue_routes(app, services):
    """
    Register queue management endpoints and start the stats refresher
    Uses a lightweight in-process stats cache to avoid hitting queue internals on every dashboard poll.
    """
    expected_endpoints = {
        "queue_stats",
        "queue_health",
        "pause_task",
        "resume_task",
        "cancel_task",
        "submit_ml_detection",
        "submit_prediction",
        "submit_data_ingestion"
    }

    already = expected_endpoints.intersection(set(app.view_functions.keys()))
    if already:
        logger.info("[ROUTES] Detected existing queue endpoints: %s — skipping registration to avoid duplicates", ", ".join(sorted(already)))
        return
    # --- End duplicate-protection ---

    queue_manager: Optional[AquaGuardQueueManager] = None
    try:
        queue_manager = services.get("queue_manager")
    except Exception:
        queue_manager = None

    if not queue_manager:
        logger.warning("[ROUTES] Queue manager not available - skipping route registration")
        return

    # Ensure background stats refresher is started once per process
    try:
        _ensure_stats_refresher_started(app, queue_manager)
    except Exception:
        logger.exception("[ROUTES] Failed to start stats refresher (non-fatal)")

    @app.route("/queue/stats", methods=["GET"])
    def queue_stats():
        """Return cached queue stats (fast) with last_updated timestamp"""
        try:
            with _stats_lock:
                value = _stats_cache.get("value")
                last = _stats_cache.get("last_updated")

            # If cache empty, fetch synchronously as fallback
            if value is None:
                try:
                    value = queue_manager.get_stats()
                    with _stats_lock:
                        _stats_cache["value"] = value
                        _stats_cache["last_updated"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
                        last = _stats_cache["last_updated"]
                except Exception:
                    logger.exception("[ROUTES] Failed to fetch queue stats fallback")
                    return jsonify({"status": "error", "message": "Failed to get stats"}), 500

            return jsonify({
                "status": "success",
                "stats": value,
                "cached_at": last
            }), 200

        except Exception as e:
            logger.exception("[ROUTES] /queue/stats error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/health", methods=["GET"])
    def queue_health():
        """Return queue API health (delegates to queue manager)"""
        try:
            health = queue_manager.get_api_health()
            return jsonify({"status": "success", "health": health, "timestamp": datetime.utcnow().isoformat()}), 200
        except Exception as e:
            logger.exception("[ROUTES] /queue/health error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/task/<task_id>/pause", methods=["POST"])
    def pause_task(task_id):
        try:
            ok = queue_manager.pause_task(task_id)
            return jsonify({"status": "success" if ok else "error", "task_id": task_id, "paused": bool(ok)}), (200 if ok else 404)
        except Exception as e:
            logger.exception("[ROUTES] pause_task error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/task/<task_id>/resume", methods=["POST"])
    def resume_task(task_id):
        try:
            ok = queue_manager.resume_task(task_id)
            return jsonify({"status": "success" if ok else "error", "task_id": task_id, "resumed": bool(ok)}), (200 if ok else 404)
        except Exception as e:
            logger.exception("[ROUTES] resume_task error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/task/<task_id>/cancel", methods=["POST"])
    def cancel_task(task_id):
        try:
            ok = queue_manager.cancel_task(task_id)
            return jsonify({"status": "success" if ok else "error", "task_id": task_id, "cancelled": bool(ok)}), (200 if ok else 404)
        except Exception as e:
            logger.exception("[ROUTES] cancel_task error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/ml/detect", methods=["POST"])
    def submit_ml_detection():
        """Submit an ML anomaly detection job via queue"""
        try:
            payload = request.get_json(silent=True) or {}
            hours_back = int(payload.get("hours_back", 24))
            min_score = float(payload.get("min_score", 50.0))
            priority = queue_manager.TaskPriority.HIGH

            task_id = queue_manager.submit_ml_anomaly_detection(
                hours_back=hours_back,
                min_score=min_score,
                priority=priority
            )

            if task_id:
                return jsonify({"status": "success", "task_id": task_id, "message": "ML detection queued"}), 200
            else:
                return jsonify({"status": "error", "message": "Failed to enqueue task (queue full?)"}), 503

        except Exception as e:
            logger.exception("[ROUTES] submit_ml_detection error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/prediction/submit", methods=["POST"])
    def submit_prediction():
        """Submit a prediction job via queue"""
        try:
            payload = request.get_json(silent=True) or {}
            prediction_type = payload.get("type", "filter_rul")
            params = payload.get("params", {})
            priority = queue_manager.TaskPriority.MEDIUM

            task_id = queue_manager.submit_prediction_task(
                prediction_type=prediction_type,
                params=params,
                priority=priority
            )

            if task_id:
                return jsonify({"status": "success", "task_id": task_id, "message": f"Prediction '{prediction_type}' queued"}), 200
            else:
                return jsonify({"status": "error", "message": "Failed to enqueue prediction (queue full?)"}), 503

        except Exception as e:
            logger.exception("[ROUTES] submit_prediction error")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route("/queue/ingest", methods=["POST"])
    def submit_data_ingestion():
        """Accept sensor payload and enqueue ingestion task"""
        try:
            data = request.get_json(force=True)
            task_id = queue_manager.submit_data_ingestion(
                data,
                priority=queue_manager.TaskPriority.HIGH
            )
            if task_id:
                return jsonify({"status": "queued", "task_id": task_id, "message": "Data ingestion queued"}), 200
            else:
                return jsonify({"status": "error", "message": "Queue full or ingestion service unavailable"}), 503
        except Exception as e:
            logger.exception("[ROUTES] submit_data_ingestion error")
            return jsonify({"status": "error", "message": str(e)}), 500

    logger.info("[ROUTES] Queue management routes registered")
