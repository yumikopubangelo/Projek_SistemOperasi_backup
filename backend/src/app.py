#!/usr/bin/env python3
"""
AquaGuard Middleware Server v7.5 - Fully Integrated Queue & Data Ingestion
Main Application Entry Point (revised)
- Safe for import (no heavy init on import)
- initialize_services() must be called explicitly
"""

import logging
import sys
import os
from flask import Flask
from flask_cors import CORS
from waitress import serve
from datetime import datetime
from typing import Dict, Any

from core.config import Config
from core.es_client import ElasticsearchClient

# Services
from services.buffering_service import BufferingService
from services.alert_service import AlertService
from services.ml_service import MLService
from services.data_ingestion import DataIngestionService
from reports.priority_fixes import apply_priority_fixes

# Routes
from routes import register_routes
from routes.prediction_routes import create_prediction_blueprint
from csv_exporter import CSVExporter

# Queue Management
from services.queue_integration import AquaGuardQueueManager, register_queue_routes

# Validate configuration early (fatal)
try:
    Config.validate()
except (ValueError, FileNotFoundError) as e:
    print(f"[FATAL] Configuration error: {e}")
    sys.exit(1)

# Setup logging (preserve file + console handlers)
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(Config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app (safe at import)
app = Flask(__name__)
CORS(app)

# Global services store (will be set by initialize_services())
# Keep as dict to avoid None referencing when imported
services: Dict[str, Any] = {}

# ==================== INITIALIZATION ====================
def initialize_services() -> Dict[str, Any]:
    """Initialize all services with proper dependency injection.
    This function is safe to call once (but is idempotent in the sense that
    it will create new services each call). It SHOULD be called explicitly
    (not on import).
    """
    logger.info("=" * 70)
    logger.info("AQUAGUARD MIDDLEWARE SERVER v7.5 - FULLY INTEGRATED")
    logger.info("=" * 70)

    # 1. Initialize Elasticsearch (Core dependency)
    logger.info("[INIT] 1/7 - Initializing Elasticsearch client...")
    es_client = None
    raw_es = None
    try:
        es_client = ElasticsearchClient()
        raw_es = es_client.client  # may trigger lazy connection
        logger.info("[OK] Elasticsearch connected")
    except Exception as e:
        logger.error(f"[ERROR] Elasticsearch initialization failed: {e}", exc_info=True)
        raw_es = None

    # 2. Initialize CSV Exporter (Optional)
    logger.info("[INIT] 2/7 - Initializing CSV Exporter...")
    csv_exporter = None
    try:
        if getattr(Config, "CSV_EXPORT_ENABLED", False):
            csv_exporter = CSVExporter(export_interval=getattr(Config, "CSV_EXPORT_INTERVAL", 3600))
            logger.info(f"[OK] CSV Exporter enabled - Reports: {csv_exporter.output_dir}")
        else:
            logger.info("[SKIP] CSV Export disabled in config")
    except Exception as e:
        logger.warning(f"[WARNING] CSV Exporter disabled: {e}", exc_info=True)

    # 3. Initialize Telegram Alert Service (Optional)
    logger.info("[INIT] 3/7 - Initializing Telegram Alert Service...")
    telegram_notifier = None
    try:
        if getattr(Config, "TELEGRAM_BOT_TOKEN", None) and getattr(Config, "TELEGRAM_CHAT_ID", None):
            telegram_notifier = AlertService(
                getattr(Config, "TELEGRAM_BOT_TOKEN"),
                getattr(Config, "TELEGRAM_CHAT_ID"),
                csv_exporter=csv_exporter
            )
            logger.info("[OK] Telegram Alert Service enabled")
        else:
            logger.info("[SKIP] Telegram not configured in Config")
    except Exception as e:
        logger.warning(f"[WARNING] Telegram notifier disabled: {e}", exc_info=True)
        telegram_notifier = None

    # 4. Initialize ML Service (Depends on es_client)
    logger.info("[INIT] 4/7 - Initializing ML Service...")
    ml_service = None
    try:
        ml_service = MLService(es_client=es_client, config=Config)
        logger.info("[OK] ML Service initialized")
    except Exception as e:
        logger.error(f"[ERROR] ML Service initialization failed: {e}", exc_info=True)
        ml_service = None

    # 5. Initialize Buffer Manager (Depends on es_client, csv_exporter)
    logger.info("[INIT] 5/7 - Initializing Buffer Manager...")
    buffer_manager = None
    try:
        buffer_manager = BufferingService(
            es_client=es_client,
            csv_exporter=csv_exporter
        )
        logger.info("[OK] Buffer Manager initialized")
    except Exception as e:
        logger.error(f"[ERROR] Buffer Manager initialization failed: {e}", exc_info=True)
        buffer_manager = None

    # 6. Initialize Data Ingestion Service (defensive about alert_service kwarg)
    logger.info("[INIT] 6/7 - Initializing Data Ingestion Service...")
    data_ingestion = None
    try:
        # Try to pass alert_service; if constructor doesn't accept it, fallback.
        try:
            data_ingestion = DataIngestionService(
                es_client=es_client,
                alert_service=telegram_notifier,
                csv_exporter=csv_exporter,
                batch_size=getattr(Config, "INGESTION_BATCH_SIZE", 100),
                flush_interval=getattr(Config, "INGESTION_FLUSH_INTERVAL", 2.0)
            )
        except TypeError:
            # constructor doesn't accept alert_service: try without it
            logger.debug("[INIT] DataIngestionService does not accept alert_service kwarg; retrying without it")
            data_ingestion = DataIngestionService(
                es_client=es_client,
                csv_exporter=csv_exporter,
                batch_size=getattr(Config, "INGESTION_BATCH_SIZE", 100),
                flush_interval=getattr(Config, "INGESTION_FLUSH_INTERVAL", 2.0)
            )
        logger.info("[OK] Data Ingestion Service initialized")
    except Exception as e:
        logger.error(f"[ERROR] Data Ingestion initialization failed: {e}", exc_info=True)
        logger.info("[WARNING] Continuing without data ingestion service")
        data_ingestion = None

    # 7. Initialize Queue Manager (Depends on all services)
    logger.info("[INIT] 7/7 - Initializing Queue Manager...")
    queue_manager = None
    try:
        # Check Redis availability first; if missing, fallback to in-process queue
        use_rq = True
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        try:
            from redis import Redis
            redis_test = Redis.from_url(redis_url)
            redis_test.ping()
            logger.info("[QUEUE] Redis available - using RQ mode")
        except Exception as redis_err:
            use_rq = False
            logger.warning(f"[QUEUE] Redis unavailable ({redis_err}) - using in-process queue")
            os.environ["DISABLE_RQ"] = "true"

        # AquaGuardQueueManager constructor is now tolerant of extra kwargs
        queue_manager = AquaGuardQueueManager(
            es_client=es_client,
            data_ingestion=data_ingestion,
            telegram_notifier=telegram_notifier,
            ml_service=ml_service,
            csv_exporter=csv_exporter,
            buffer_manager=buffer_manager,
            use_rq=use_rq
        )
        logger.info("[OK] Queue Manager initialized")
    except Exception as e:
        logger.error(f"[ERROR] Queue Manager initialization failed: {e}", exc_info=True)
        logger.info("[WARNING] Continuing without queue manager")
        queue_manager = None

    # Build services dict for use by routes and other modules
    svc = {
        "es_client": es_client,                # wrapper, may be None
        "es": raw_es,                          # raw client (may be None)
        "buffer_manager": buffer_manager,
        "telegram_notifier": telegram_notifier,
        "ml_service": ml_service,
        "csv_exporter": csv_exporter,
        "data_ingestion": data_ingestion,
        "queue_manager": queue_manager,
        "config": Config,
    }

    return svc


# ==================== ROUTES REGISTRATION (explicit) ====================
def register_all_routes(app_obj: Flask, svc: Dict[str, Any]) -> None:
    """Register routes & blueprints. Call this once after initialize_services()."""
    # Register core routes
    register_routes(app_obj, svc)

    # Prediction blueprint
    try:
        prediction_bp = create_prediction_blueprint(svc.get('es') or svc.get('es_client'))
        app_obj.register_blueprint(prediction_bp)
    except Exception:
        logger.exception("[ROUTES] Failed to register prediction blueprint (non-fatal)")

    # Apply priority fixes (override slow routes)
    try:
        apply_priority_fixes(app_obj, svc)
    except Exception:
        logger.exception("[ROUTES] apply_priority_fixes failed (non-fatal)")

    # Register queue routes if queue manager present
    try:
        if svc.get('queue_manager'):
            register_queue_routes(app_obj, svc)
        else:
            logger.warning("[WARNING] Queue routes not registered (manager unavailable)")
    except Exception:
        logger.exception("[ROUTES] register_queue_routes failed (non-fatal)")

    logger.info("[OK] All routes registered")


# ==================== GRACEFUL SHUTDOWN HANDLER ====================
def graceful_shutdown(signum=None, frame=None) -> None:
    """Handle graceful shutdown; assumes services global is populated when called."""
    global services
    logger.info("=" * 70)
    logger.info("[SHUTDOWN] Graceful shutdown initiated...")
    logger.info("=" * 70)

    try:
        # 1. Stop data ingestion first (stop new data)
        if services.get('data_ingestion'):
            logger.info("[SHUTDOWN] Step 1 - Stopping data ingestion...")
            try:
                services['data_ingestion'].shutdown()
            except Exception as e:
                logger.warning(f"[SHUTDOWN] Data ingestion shutdown error: {e}")

        # 2. Shutdown queue manager (finish pending tasks)
        if services.get('queue_manager'):
            logger.info("[SHUTDOWN] Step 2 - Stopping queue manager...")
            try:
                services['queue_manager'].shutdown()
            except Exception as e:
                logger.warning(f"[SHUTDOWN] Queue manager shutdown error: {e}")

        # 3. Flush buffer manager (save pending data)
        if services.get('buffer_manager'):
            logger.info("[SHUTDOWN] Step 3 - Flushing buffer...")
            try:
                pending = services['buffer_manager'].force_flush()
                logger.info(f"[SHUTDOWN] Flushed {pending} pending documents")
            except Exception as e:
                logger.warning(f"[SHUTDOWN] Buffer flush error: {e}")

        # 4. Shutdown alert service
        if services.get('telegram_notifier'):
            logger.info("[SHUTDOWN] Step 4 - Stopping alert service...")
            try:
                services['telegram_notifier'].shutdown()
            except Exception as e:
                logger.warning(f"[SHUTDOWN] Alert service shutdown error: {e}")

        # 5. Shutdown buffer service
        if services.get('buffer_manager'):
            logger.info("[SHUTDOWN] Step 5 - Stopping buffer service...")
            try:
                services['buffer_manager'].shutdown()
            except Exception as e:
                logger.warning(f"[SHUTDOWN] Buffer manager shutdown error: {e}")

        # 6. Close ES client if present
        if services.get('es_client'):
            try:
                services['es_client'].close()
            except Exception as e:
                logger.warning(f"[SHUTDOWN] ES client close error: {e}")

    except Exception:
        logger.exception("[SHUTDOWN] Unexpected error during shutdown")

    logger.info("=" * 70)
    logger.info("[SHUTDOWN] Complete - Server stopped gracefully")
    logger.info("=" * 70)
    # exit process
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)


# ==================== SIMPLE API ENDPOINTS (safe while services may be empty) ====================
@app.route("/system/status", methods=["GET"])
def system_status():
    """Get comprehensive system status (best-effort; non-blocking)."""
    from flask import jsonify

    try:
        svc = services or {}
        es_ping = False
        try:
            if svc.get('es'):
                es_ping = bool(svc['es'].ping())
        except Exception:
            es_ping = False

        status = {
            "status": "operational" if es_ping else "degraded",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "elasticsearch": bool(svc.get('es')),
                "buffer_manager": svc.get('buffer_manager') is not None,
                "data_ingestion": svc.get('data_ingestion') is not None,
                "queue_manager": svc.get('queue_manager') is not None,
                "ml_service": svc.get('ml_service') is not None,
                "telegram": svc.get('telegram_notifier') is not None,
                "csv_export": svc.get('csv_exporter') is not None,
            },
            "statistics": {}
        }

        # Try to get stats non-blocking / best-effort
        try:
            if svc.get('data_ingestion'):
                status['statistics']['ingestion'] = svc['data_ingestion'].get_stats()
        except Exception:
            logger.debug("[SYSTEM STATUS] ingestion stats error", exc_info=True)

        try:
            if svc.get('queue_manager'):
                status['statistics']['queue'] = svc['queue_manager'].get_stats()
        except Exception:
            logger.debug("[SYSTEM STATUS] queue stats error", exc_info=True)

        try:
            if svc.get('buffer_manager'):
                status['statistics']['buffer'] = svc['buffer_manager'].get_stats()
        except Exception:
            logger.debug("[SYSTEM STATUS] buffer stats error", exc_info=True)

        return jsonify(status), 200

    except Exception as e:
        logger.error(f"[SYSTEM STATUS] Error: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# Demo endpoints (use services at runtime; return 503 if not initialized)
@app.route("/demo/queue/ml", methods=["GET"])
def demo_ml_task():
    from flask import jsonify
    from services.queue_service import TaskPriority

    qm = services.get('queue_manager')
    if not qm:
        return jsonify({"status": "error", "message": "Queue manager not available"}), 503

    task_id = qm.submit_ml_anomaly_detection(hours_back=24, min_score=50.0, priority=TaskPriority.HIGH)
    return jsonify({"status": "success", "task_id": task_id}), 200


@app.route("/demo/queue/prediction", methods=["GET"])
def demo_prediction_task():
    from flask import jsonify, request
    from services.queue_service import TaskPriority

    qm = services.get('queue_manager')
    if not qm:
        return jsonify({"status": "error", "message": "Queue manager not available"}), 503

    prediction_type = request.args.get("type", "filter_rul")
    task_id = qm.submit_prediction_task(prediction_type=prediction_type, params={"hours_back": 48}, priority=TaskPriority.MEDIUM)
    return jsonify({"status": "success", "task_id": task_id}), 200


# ==================== MAIN STARTUP (explicit) ====================
def main():
    """Explicit entrypoint to initialize services, register routes, attach signals, and run server."""
    global services

    # initialize services once
    services = initialize_services()

    # register routes and blueprints now that services available
    register_all_routes(app, services)

    # attach graceful shutdown signals now that services exist
    import signal as _signal
    _signal.signal(_signal.SIGINT, lambda s, f: graceful_shutdown(s, f))
    _signal.signal(_signal.SIGTERM, lambda s, f: graceful_shutdown(s, f))

    # start server
    logger.info("=" * 70)
    logger.info(f"[START] Server starting on {Config.SERVER_HOST}:{Config.SERVER_PORT}")
    logger.info("=" * 70)
    logger.info("[READY] Server is ready to accept requests")
    logger.info("=" * 70)

    serve(
        app,
        host=Config.SERVER_HOST,
        port=Config.SERVER_PORT,
        threads=getattr(Config, "SERVER_THREADS", 50),
        channel_timeout=getattr(Config, "CHANNEL_TIMEOUT", 120),
        connection_limit=getattr(Config, "CONNECTION_LIMIT", 1000),
        cleanup_interval=getattr(Config, "CLEANUP_INTERVAL", 30)
    )


if __name__ == "__main__":
    main()
