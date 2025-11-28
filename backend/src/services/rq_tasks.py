"""
RQ task functions executed by workers.

This module is expected to be imported by the worker process.
The worker should set `services.rq_tasks.SERVICES = <dict>` after
calling app.initialize_services() so tasks can access shared services.

Primary exported function:
 - execute_callable(func_path: str, args: tuple, kwargs: dict)

Also includes convenient wrappers used by the RQ enqueue helpers.
"""

import importlib
import logging
from typing import Any, Dict, Optional, List

logger = logging.getLogger(__name__)

# Worker will inject this mapping at startup:
SERVICES: Optional[Dict[str, Any]] = None


def _resolve_callable(func_path: str):
    """
    Resolve a callable from a func_path string.

    Supported func_path formats:
      - "package.module.func"             -> returns function object
      - "package.module:func"             -> same as above
      - "service_name.method"             -> if 'service_name' exists in SERVICES, return bound method
    """
    if not func_path or not isinstance(func_path, str):
        raise ValueError("Invalid func_path")

    # allow "module:func" syntax
    if ":" in func_path:
        module_path, func_name = func_path.split(":", 1)
    else:
        # last dot separates module and attribute
        if "." not in func_path:
            raise ValueError("func_path must be a dotted path or module:attr")
        module_path, func_name = func_path.rsplit(".", 1)

    # 1) Direct SERVICES lookup
    if SERVICES:
        # Check if this is a service method call (e.g., "queue_manager.method_name")
        if module_path in SERVICES and hasattr(SERVICES[module_path], func_name):
            return getattr(SERVICES[module_path], func_name), f"bound method {module_path}.{func_name}"

    # 2) Fallback: import the module normally and get attribute
    try:
        module = importlib.import_module(module_path)
    except Exception as e:
        # Extra fallback: if SERVICES contains instance keyed by module_path (rare)
        if SERVICES and module_path in SERVICES and hasattr(SERVICES[module_path], func_name):
            return getattr(SERVICES[module_path], func_name), f"bound method {module_path}.{func_name}"
        raise

    if not hasattr(module, func_name):
        raise AttributeError(f"Module '{module_path}' has no attribute '{func_name}'")

    return getattr(module, func_name), f"callable {module_path}.{func_name}"


# --------------------------
# Core executor used by RQ
# --------------------------
def execute_callable(func_path: str, args: Optional[tuple] = None, kwargs: Optional[dict] = None) -> Any:
    """
    Generic executor used by RQ jobs.
    - func_path: dotted path to a function or service method, e.g. "my.module.fn" or "service_name.method"
    - args, kwargs: passed to the function
    Returns the callable's return value or raises (RQ will capture & record).
    """
    args = args or ()
    kwargs = kwargs or {}

    logger.debug("[RQ TASK] execute_callable: resolving %s", func_path)
    try:
        fn, desc = _resolve_callable(func_path)
    except Exception as e:
        logger.exception("[RQ TASK] Failed to resolve callable '%s': %s", func_path, e)
        raise

    logger.info("[RQ TASK] Executing %s with args=%s kwargs=%s", desc, args, kwargs)
    try:
        return fn(*args, **kwargs)
    except Exception:
        logger.exception("[RQ TASK] Exception while executing %s", func_path)
        raise


# --------------------------
# Standalone Health Check Tasks
# --------------------------
def es_health_check() -> bool:
    """
    Standalone Elasticsearch health check task.
    Uses SERVICES['es'] to perform ping.
    """
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    
    es_client = SERVICES.get("es")
    if not es_client:
        logger.error("[ES HEALTH] ES client not available in SERVICES")
        return False
    
    try:
        result = bool(es_client.ping())
        logger.info(f"[ES HEALTH] Ping result: {result}")
        return result
    except Exception as e:
        logger.exception(f"[ES HEALTH] Ping failed: {e}")
        return False


def system_health_check() -> Dict[str, Any]:
    """
    Comprehensive system health check task.
    Checks all critical services.
    """
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    
    health = {
        "timestamp": None,
        "elasticsearch": False,
        "buffer_manager": False,
        "data_ingestion": False,
        "ml_service": False,
        "queue_manager": False
    }
    
    try:
        from datetime import datetime, timezone
        health["timestamp"] = datetime.now(timezone.utc).isoformat()
        
        # Check Elasticsearch
        es_client = SERVICES.get("es")
        if es_client:
            try:
                health["elasticsearch"] = bool(es_client.ping())
            except Exception:
                pass
        
        # Check other services
        health["buffer_manager"] = SERVICES.get("buffer_manager") is not None
        health["data_ingestion"] = SERVICES.get("data_ingestion") is not None
        health["ml_service"] = SERVICES.get("ml_service") is not None
        health["queue_manager"] = SERVICES.get("queue_manager") is not None
        
        logger.info(f"[SYSTEM HEALTH] Check completed: {health}")
        return health
        
    except Exception as e:
        logger.exception(f"[SYSTEM HEALTH] Check failed: {e}")
        raise


# --------------------------
# Data Ingestion Tasks
# --------------------------
def data_ingestion_ingest(data: Dict[str, Any]) -> bool:
    """Worker wrapper to ingest single record via SERVICES['data_ingestion']"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    data_ing = SERVICES.get("data_ingestion")
    if not data_ing:
        raise RuntimeError("DataIngestionService not available in SERVICES")
    try:
        return data_ing.ingest(data)
    except Exception:
        logger.exception("[RQ TASK] data_ingestion_ingest failed")
        raise


def data_ingestion_ingest_batch(batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Worker wrapper to ingest batch"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    data_ing = SERVICES.get("data_ingestion")
    if not data_ing:
        raise RuntimeError("DataIngestionService not available in SERVICES")
    try:
        return data_ing.ingest_batch(batch)
    except Exception:
        logger.exception("[RQ TASK] data_ingestion_ingest_batch failed")
        raise


# --------------------------
# ML Tasks
# --------------------------
def ml_anomaly_detection(hours_back: int, min_score: float) -> Dict[str, Any]:
    """Execute ML anomaly detection"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    ml_service = SERVICES.get("ml_service")
    if not ml_service:
        raise RuntimeError("MLService not available in SERVICES")
    try:
        return ml_service.get_anomalies(size=100, min_score=min_score, hours_back=hours_back)
    except Exception:
        logger.exception("[RQ TASK] ml_anomaly_detection failed")
        raise


def ml_status_check() -> Dict[str, Any]:
    """Check ML service status"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    ml_service = SERVICES.get("ml_service")
    if not ml_service:
        raise RuntimeError("MLService not available in SERVICES")
    try:
        return ml_service.get_status()
    except Exception:
        logger.exception("[RQ TASK] ml_status_check failed")
        raise


def ml_summary(hours_back: int = 24) -> Dict[str, Any]:
    """Generate ML summary"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    ml_service = SERVICES.get("ml_service")
    if not ml_service:
        raise RuntimeError("MLService not available in SERVICES")
    try:
        return ml_service.get_summary(hours_back=hours_back)
    except Exception:
        logger.exception("[RQ TASK] ml_summary failed")
        raise


# --------------------------
# Prediction Tasks
# --------------------------
def prediction_task(prediction_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute prediction task"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    
    try:
        # Use queue_manager's prediction helper if available
        qm = SERVICES.get("queue_manager")
        if qm and hasattr(qm, "_run_prediction"):
            return qm._run_prediction(prediction_type=prediction_type, params=params)
        
        # Fallback: use prediction_engine directly
        from prediction_engine import WaterQualityPredictor
        predictor = WaterQualityPredictor()
        
        # This is a simplified version - real implementation should fetch data from ES
        return predictor.predict_next_value(
            [], 
            parameter=params.get("parameter", "tds_ppm"), 
            hours_ahead=params.get("hours_ahead", 1)
        )
    except Exception:
        logger.exception("[RQ TASK] prediction_task failed")
        raise


# --------------------------
# CSV Export Tasks
# --------------------------
def csv_export(export_type: str, data: Dict[str, Any]) -> bool:
    """Execute CSV export"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    csv_exporter = SERVICES.get("csv_exporter")
    if not csv_exporter:
        raise RuntimeError("CSV exporter not configured")
    try:
        if export_type == "sensor_data":
            csv_exporter.log_sensor_data(data)
        elif export_type == "anomaly":
            csv_exporter.log_anomaly(
                data.get("type"), 
                data.get("value"), 
                data.get("threshold"), 
                data.get("severity")
            )
        elif export_type == "adaptive_stats":
            csv_exporter.export_adaptive_stats(data)
        else:
            raise RuntimeError(f"Unknown export_type {export_type}")
        return True
    except Exception:
        logger.exception("[RQ TASK] csv_export failed")
        raise


# --------------------------
# Buffer Management Tasks
# --------------------------
def buffer_flush() -> int:
    """Force flush buffer manager"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    bm = SERVICES.get("buffer_manager")
    if not bm:
        raise RuntimeError("Buffer manager not available")
    try:
        return bm.force_flush()
    except Exception:
        logger.exception("[RQ TASK] buffer_flush failed")
        raise


# --------------------------
# Bulk Processing Tasks
# --------------------------
def bulk_data_processing(batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process bulk data batch"""
    if not SERVICES:
        raise RuntimeError("SERVICES not initialized in worker")
    
    try:
        # Prefer queue_manager helper if exists
        qm = SERVICES.get("queue_manager")
        if qm and hasattr(qm, "_process_bulk_data"):
            return qm._process_bulk_data(batch)
        
        # Fallback: use ES client directly
        es_client = SERVICES.get("es")
        if es_client and hasattr(es_client, "bulk_index"):
            succ, fail = es_client.bulk_index(batch)
            return {"status": "success", "success": succ, "failed": fail}
        
        raise RuntimeError("No bulk processing path available")
    except Exception:
        logger.exception("[RQ TASK] bulk_data_processing failed")
        raise


# --------------------------
# Utility: List all available tasks
# --------------------------
def list_available_tasks() -> List[str]:
    """
    Return list of all task functions available in this module.
    Useful for debugging and documentation.
    """
    import inspect
    
    tasks = []
    for name, obj in globals().items():
        if callable(obj) and not name.startswith("_") and inspect.isfunction(obj):
            tasks.append(name)
    
    return sorted(tasks)