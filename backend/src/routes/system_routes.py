from flask import jsonify, request
from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)


def register_system_routes(app, services):
    es = services['es']
    buffer_manager = services['buffer_manager']
    telegram_notifier = services.get('telegram_notifier')
    
    from core.config import Config

    @app.route("/health", methods=["GET"])
    def health():
        """Health check with timeout protection"""
        try:
            # Set timeout untuk ES ping
            start = time.time()
            es_healthy = es.ping()
            elapsed = time.time() - start
            
            return jsonify({
                "status": "healthy" if es_healthy else "degraded",
                "elasticsearch": es_healthy,
                "response_time_ms": round(elapsed * 1000, 2),
                "timestamp": datetime.now().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"[HEALTH ERROR] {e}", exc_info=True)
            return jsonify({
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }), 500

    @app.route("/stats", methods=["GET"])
    def get_stats():
        """Get system statistics with fast fallback"""
        try:
            logger.info("[STATS] Fetching system statistics...")
            
            # Quick buffer stats (no DB call)
            buffer_stats = {"current_size": 0, "total_sent": 0, "error": None}
            if buffer_manager:
                try:
                    buffer_stats = buffer_manager.get_stats()
                except Exception as e:
                    logger.error(f"[STATS] Buffer error: {e}")
                    buffer_stats["error"] = str(e)
            
            # Quick telegram stats (no DB call)
            telegram_stats = {"total_sent": 0, "total_failed": 0, "status": "unknown"}
            if telegram_notifier:
                try:
                    telegram_stats = telegram_notifier.get_stats()
                except Exception as e:
                    logger.error(f"[STATS] Telegram error: {e}")
                    telegram_stats["error"] = str(e)
            else:
                telegram_stats["status"] = "not_configured"
            
            response = {
                "buffer": buffer_stats,
                "telegram": telegram_stats,
                "timestamp": datetime.now().isoformat()
            }
            
            return jsonify(response), 200
            
        except Exception as e:
            logger.error(f"[STATS ERROR] {e}", exc_info=True)
            return jsonify({
                "status": "error",
                "message": str(e),
                "buffer": {"current_size": 0, "total_sent": 0},
                "telegram": {"total_sent": 0, "total_failed": 0},
                "timestamp": datetime.now().isoformat()
            }), 500

    @app.route("/data/latest", methods=["GET"])
    def latest_data():
        """Get latest sensor data with timeout protection"""
        try:
            # Flush buffer dengan timeout
            try:
                buffer_manager.force_flush()
            except Exception as e:
                logger.warning(f"[LATEST] force_flush failed: {e}")

            # ES search dengan timeout pendek
            result = es.search(
                index=Config.ELASTIC_INDEX,
                size=1,
                sort=[{"@timestamp": {"order": "desc"}}],
                request_timeout=5  # 5 detik timeout
            )

            if result['hits']['total']['value'] == 0:
                return jsonify({
                    "status": "error", 
                    "message": "No data available"
                }), 404

            return jsonify(result['hits']['hits'][0]['_source']), 200

        except Exception as e:
            logger.error(f"[LATEST DATA ERROR] {e}", exc_info=True)
            return jsonify({
                "status": "error", 
                "message": str(e)
            }), 500

    @app.route("/data/historical", methods=["GET"])
    def historical_data():
        """Get historical data with timeout protection"""
        try:
            size = int(request.args.get("size", 100))
            size = max(1, min(size, 1000))

            # Flush buffer
            try:
                buffer_manager.force_flush()
            except Exception as e:
                logger.warning(f"[HISTORICAL] force_flush failed: {e}")

            # ES search dengan timeout
            result = es.search(
                index=Config.ELASTIC_INDEX,
                size=size,
                sort=[{"@timestamp": {"order": "desc"}}],
                request_timeout=10  # 10 detik timeout
            )

            hits = result['hits']['hits']
            if not hits:
                return jsonify({
                    "status": "error",
                    "message": "No historical data available"
                }), 404

            # Sort chronologically
            data = [h['_source'] for h in reversed(hits)]

            return jsonify({
                "status": "success",
                "count": len(data),
                "data": data
            }), 200

        except Exception as e:
            logger.error(f"[HISTORICAL DATA ERROR] {e}", exc_info=True)
            return jsonify({
                "status": "error", 
                "message": str(e)
            }), 500