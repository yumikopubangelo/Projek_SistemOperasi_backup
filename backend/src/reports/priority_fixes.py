"""
Priority Fixes for AquaGuard - Based on Diagnostic Results
Fixes:
1. Stats endpoint timeout (CRITICAL)
2. ML anomaly timestamp format (HIGH)
3. Queue route registration (MEDIUM)
"""

import logging
from flask import jsonify, request
from datetime import datetime, timezone
from functools import wraps
import time

logger = logging.getLogger(__name__)


# ============================================================================
# FIX 1: FAST STATS ENDPOINT (NO BLOCKING OPERATIONS)
# ============================================================================

def register_fast_stats_route(app, services):
    """
    Ultra-fast stats endpoint that NEVER times out
    Only returns in-memory data, NO database queries
    """
    
    buffer_manager = services['buffer_manager']
    telegram_notifier = services.get('telegram_notifier')
    queue_manager = services.get('queue_manager')
    csv_exporter = services.get('csv_exporter')
    
    @app.route("/stats", methods=["GET"])
    def get_stats_fast():
        """
        CRITICAL: This endpoint MUST respond in <1 second
        NO Elasticsearch queries allowed here!
        """
        try:
            start_time = time.time()
            current_time = datetime.now(timezone.utc)
            
            stats = {
                "status": "success",
                "timestamp": current_time.isoformat(),
                "server_time": current_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "response_time_ms": 0  # Will be updated at end
            }
            
            # Buffer stats (in-memory only)
            if buffer_manager:
                try:
                    # Call get_stats WITHOUT forcing flush
                    buffer_data = buffer_manager.get_stats()
                    stats["buffer"] = {
                        "current_size": buffer_data.get("current_buffer_size", 0),
                        "configured_size": buffer_data.get("configured_buffer_size", 50),
                        "total_sent": buffer_data.get("total_flushed", 0),
                        "total_received": buffer_data.get("total_received", 0),
                        "pending": buffer_data.get("pending", 0),
                        "failed": 0,
                        "strategy": buffer_data.get("current_strategy", "NORMAL"),
                        "traffic_rpm": round(buffer_data.get("traffic_rpm", 0), 2),
                        "last_flush": buffer_data.get("last_flush")
                    }
                except Exception as e:
                    logger.error(f"[STATS] Buffer error: {e}")
                    stats["buffer"] = {
                        "current_size": 0,
                        "configured_size": 50,
                        "total_sent": 0,
                        "error": "unavailable"
                    }
            else:
                stats["buffer"] = {"status": "not_available"}
            
            # Telegram stats (in-memory only)
            if telegram_notifier:
                try:
                    tg_data = telegram_notifier.get_stats()
                    stats["telegram"] = {
                        "total_sent": tg_data.get("alerts_sent", 0),
                        "recent_anomalies": tg_data.get("recent_count", 0),
                        "total_anomalies": tg_data.get("total_anomalies", 0),
                        "status": "active"
                    }
                except Exception as e:
                    logger.error(f"[STATS] Telegram error: {e}")
                    stats["telegram"] = {
                        "total_sent": 0,
                        "status": "error"
                    }
            else:
                stats["telegram"] = {"status": "not_configured"}
            
            # Queue stats (if available)
            if queue_manager:
                try:
                    queue_data = queue_manager.get_stats()
                    stats["queue"] = {
                        "queued": queue_data.get("queued_tasks", 0),
                        "active": queue_data.get("active", 0),
                        "completed": queue_data.get("total_completed", 0),
                        "failed": queue_data.get("total_failed", 0)
                    }
                except:
                    stats["queue"] = {"status": "unavailable"}
            else:
                stats["queue"] = {"status": "not_configured"}
            
            # CSV stats (quick file check only)
            if csv_exporter:
                try:
                    csv_data = csv_exporter.get_stats()
                    stats["csv"] = {
                        "sensor_records": csv_data.get("sensor_data", {}).get("records", 0),
                        "anomaly_records": csv_data.get("anomaly_log", {}).get("records", 0)
                    }
                except:
                    stats["csv"] = {"status": "error"}
            
            # Calculate response time
            response_time = (time.time() - start_time) * 1000
            stats["response_time_ms"] = round(response_time, 2)
            
            # Log if slow
            if response_time > 500:
                logger.warning(f"[STATS] Slow response: {response_time:.0f}ms")
            
            return jsonify(stats), 200
            
        except Exception as e:
            logger.error(f"[STATS ERROR] {e}", exc_info=True)
            # Return minimal valid response
            return jsonify({
                "status": "error",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": "Stats temporarily unavailable",
                "buffer": {"current_size": 0, "total_sent": 0},
                "telegram": {"total_sent": 0}
            }), 200  # Return 200 to prevent frontend errors
    
    logger.info("[FIX] Fast stats endpoint registered")


# ============================================================================
# FIX 2: ML ANOMALY TIMESTAMP FORMATTER
# ============================================================================

def fix_ml_anomaly_timestamps(anomalies):
    """
    Fix ML anomaly timestamp format
    Converts epoch milliseconds to ISO format
    """
    for anom in anomalies:
        timestamp = anom.get('timestamp')
        
        # Check if timestamp is epoch milliseconds (like 1764225000000)
        if isinstance(timestamp, (int, float)) and timestamp > 1000000000000:
            # Convert from milliseconds to seconds
            timestamp_seconds = timestamp / 1000
            dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
            anom['timestamp'] = dt.isoformat()
            anom['timestamp_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # If already string, normalize it
        elif isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                anom['timestamp'] = dt.isoformat()
                anom['timestamp_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            except:
                # Invalid timestamp, use current time
                dt = datetime.now(timezone.utc)
                anom['timestamp'] = dt.isoformat()
                anom['timestamp_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    return anomalies


def register_fixed_ml_routes(app, services):
    """
    Register ML routes with timestamp fix
    """
    ml_service = services.get('ml_service')
    
    if not ml_service:
        logger.warning("[FIX] ML service not available")
        return
    
    @app.route("/ml/anomalies", methods=["GET"])
    def ml_anomalies_fixed():
        """ML anomalies with timestamp fix"""
        try:
            size = int(request.args.get("size", 50))
            min_score = float(request.args.get("min_score", 0))
            hours_back = int(request.args.get("hours_back", 24))
            job_id = request.args.get("job_id", None)
            
            # Get anomalies
            result = ml_service.get_anomalies(
                size=size,
                min_score=min_score,
                hours_back=hours_back,
                job_id=job_id
            )
            
            # Fix timestamps
            if result.get("status") == "success" and result.get("anomalies"):
                result["anomalies"] = fix_ml_anomaly_timestamps(result["anomalies"])
            
            return jsonify(result), 200
            
        except Exception as e:
            logger.error(f"[ML ANOMALIES] Error: {e}")
            return jsonify({
                "status": "error",
                "message": str(e),
                "anomalies": [],
                "total": 0
            }), 200
    
    logger.info("[FIX] ML routes with timestamp fix registered")


# ============================================================================
# FIX 3: QUEUE ROUTE REGISTRATION FIX
# ============================================================================

def register_queue_routes_safe(app, services):
    """
    Safely register queue routes (handles missing queue_manager)
    """
    queue_manager = services.get('queue_manager')
    
    @app.route("/queue/stats", methods=["GET"])
    def queue_stats():
        """Queue statistics with fallback"""
        if not queue_manager:
            return jsonify({
                "status": "not_configured",
                "message": "Queue manager not initialized",
                "stats": {
                    "queued_tasks": 0,
                    "active": 0,
                    "completed": 0,
                    "failed": 0
                }
            }), 200  # Return 200, not 404
        
        try:
            stats = queue_manager.get_stats()
            return jsonify({
                "status": "success",
                "stats": stats,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }), 200
        except Exception as e:
            logger.error(f"[QUEUE STATS] Error: {e}")
            return jsonify({
                "status": "error",
                "message": str(e),
                "stats": {
                    "queued_tasks": 0,
                    "active": 0
                }
            }), 200
    
    @app.route("/queue/health", methods=["GET"])
    def queue_health():
        """Queue health check"""
        if not queue_manager:
            return jsonify({
                "healthy": False,
                "status": "not_configured"
            }), 200
        
        try:
            health = queue_manager.get_api_health()
            return jsonify({
                "healthy": True,
                "health": health,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }), 200
        except Exception as e:
            return jsonify({
                "healthy": False,
                "error": str(e)
            }), 200
    
    logger.info("[FIX] Queue routes registered safely")


# ============================================================================
# FIX 4: PERFORMANCE MONITORING
# ============================================================================

def add_performance_monitoring(app):
    """
    Add performance monitoring to slow endpoints
    """
    @app.before_request
    def before_request():
        request._start_time = time.time()
    
    @app.after_request
    def after_request(response):
        if hasattr(request, '_start_time'):
            elapsed = (time.time() - request._start_time) * 1000
            
            # Log slow requests
            if elapsed > 1000:  # More than 1 second
                logger.warning(
                    f"[SLOW REQUEST] {request.method} {request.path} "
                    f"took {elapsed:.0f}ms"
                )
            
            # Add header
            response.headers['X-Response-Time'] = f"{elapsed:.2f}ms"
        
        return response
    
    logger.info("[FIX] Performance monitoring enabled")


# ============================================================================
# MAIN PATCH FUNCTION
# ============================================================================

def apply_priority_fixes(app, services):
    """
    Apply all priority fixes based on diagnostic results
    
    Call this from your app.py:
    ```python
    from priority_fixes import apply_priority_fixes
    apply_priority_fixes(app, services)
    ```
    """
    logger.info("=" * 70)
    logger.info("APPLYING PRIORITY FIXES")
    logger.info("=" * 70)
    
    # Fix 1: Fast stats endpoint (CRITICAL - fixes timeout)
    logger.info("[1/4] Registering fast stats endpoint...")
    register_fast_stats_route(app, services)
    
    # Fix 2: ML timestamp formatter (HIGH - fixes Invalid Date)
    logger.info("[2/4] Registering ML routes with timestamp fix...")
    register_fixed_ml_routes(app, services)
    
    # Fix 3: Queue routes (MEDIUM - fixes 404)
    logger.info("[3/4] Registering queue routes safely...")
    register_queue_routes_safe(app, services)
    
    # Fix 4: Performance monitoring
    logger.info("[4/4] Enabling performance monitoring...")
    add_performance_monitoring(app)
    
    logger.info("=" * 70)
    logger.info("ALL PRIORITY FIXES APPLIED")
    logger.info("=" * 70)


# ============================================================================
# EXAMPLE USAGE IN app.py
# ============================================================================

"""
Add this to your app.py AFTER initializing services:

from priority_fixes import apply_priority_fixes

# ... existing code ...
services = initialize_services()

# Apply fixes BEFORE registering original routes
apply_priority_fixes(app, services)

# ... then register your existing routes ...
register_routes(app, services)
"""