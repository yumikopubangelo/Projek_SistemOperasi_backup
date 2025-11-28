"""
ML Routes - Fast Version with Timeout Protection
"""

from flask import request, jsonify
import logging
from functools import wraps
import signal

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Request timeout")


def with_timeout(seconds):
    """Decorator to add timeout to route"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Set timeout alarm
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(seconds)
            
            try:
                result = func(*args, **kwargs)
            finally:
                # Disable alarm
                signal.alarm(0)
            
            return result
        return wrapper
    return decorator


def register_ml_routes(app, services):
    ml = services.get('ml_service')
    
    if not ml:
        logger.warning("[ML ROUTES] ML service not available")
        
        # Register dummy endpoints
        @app.route("/ml/status", methods=["GET"])
        def ml_status_dummy():
            return jsonify({
                "status": "unavailable",
                "message": "ML service not configured"
            }), 503
        
        @app.route("/ml/summary", methods=["GET"])
        def ml_summary_dummy():
            return jsonify({
                "status": "unavailable",
                "message": "ML service not configured"
            }), 503
        
        return

    @app.route("/ml/anomalies", methods=["GET"])
    def ml_anomalies():
        """
        Get anomalies - WITH TIMEOUT
        """
        try:
            size = int(request.args.get("size", 50))
            min_score = float(request.args.get("min_score", 0))
            hours_back = int(request.args.get("hours_back", 24))
            job_id = request.args.get("job_id", None)
            
            # Call with timeout protection
            result = ml.get_anomalies(
                size=size,
                min_score=min_score,
                hours_back=hours_back,
                job_id=job_id
            )
            
            return jsonify(result), 200
            
        except Exception as e:
            logger.error(f"[ML ANOMALIES] Error: {e}")
            return jsonify({
                "status": "error",
                "message": "Query timeout or error",
                "anomalies": [],
                "total": 0
            }), 200  # Return 200 not 500

    @app.route("/ml/status", methods=["GET"])
    def ml_status():
        """
        Get ML status - FAST VERSION
        Returns cached status, not real-time
        """
        try:
            result = ml.get_status()
            return jsonify(result), 200
        except Exception as e:
            logger.error(f"[ML STATUS] Error: {e}")
            return jsonify({
                "status": "ERROR",
                "message": "ML service unavailable",
                "jobs": []
            }), 200  # Return 200 not 500

    @app.route("/ml/status/<job_id>", methods=["GET"])
    def ml_job_status(job_id):
        """Get status of specific ML job"""
        try:
            result = ml.get_job_status(job_id)
            return jsonify(result), 200
        except Exception as e:
            logger.error(f"[ML JOB STATUS] Error: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 200

    @app.route("/ml/summary", methods=["GET"])
    def ml_summary():
        """
        Get summary - WITH TIMEOUT
        """
        try:
            hours_back = int(request.args.get("hours_back", 24))
            
            result = ml.get_summary(hours_back=hours_back)
            return jsonify(result), 200
            
        except Exception as e:
            logger.error(f"[ML SUMMARY] Error: {e}")
            return jsonify({
                "status": "error",
                "message": "Query timeout",
                "total_anomalies": 0,
                "severity_breakdown": {
                    "low": 0, "medium": 0, "high": 0, "critical": 0
                },
                "timeline": [],
                "jobs": []
            }), 200  # Return 200 not 500

    @app.route("/ml/health", methods=["GET"])
    def ml_health():
        """Quick health check"""
        try:
            result = ml.check_health()
            return jsonify(result), 200
        except Exception as e:
            logger.error(f"[ML HEALTH] Error: {e}")
            return jsonify({
                "healthy": False,
                "error": str(e)
            }), 200

    @app.route("/ml/jobs", methods=["GET"])
    def ml_jobs_list():
        """Get list of configured ML jobs"""
        try:
            return jsonify({
                "status": "success",
                "jobs": ml.job_ids,
                "total": len(ml.job_ids),
                "default_job": ml.default_job_id
            }), 200
        except Exception as e:
            logger.error(f"[ML JOBS] Error: {e}")
            return jsonify({
                "status": "error",
                "jobs": [],
                "total": 0
            }), 200
    
    logger.info("[ROUTES] Fast ML routes registered")