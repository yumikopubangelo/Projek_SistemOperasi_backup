from flask import request, jsonify
import logging

logger = logging.getLogger(__name__)


def register_sensor_routes(app, services):
    buffer_manager = services['buffer_manager']
    telegram_notifier = services.get('telegram_notifier')  # Use .get() to handle None
    csv_exporter = services.get('csv_exporter')

    @app.route("/sensor", methods=["POST"])
    def receive_sensor_data():
        try:
            data = request.get_json(force=True)

            # Basic validation
            for field in ['tds_ppm', 'kekeruhan_ntu', 'suhu_celsius']:
                if field not in data:
                    return jsonify({"status": "error", "message": f"Missing field: {field}"}), 400

            # Adaptive buffering
            buffer_manager.add(data)

            # CSV logging
            if csv_exporter:
                csv_exporter.log_sensor_data(data)

            return jsonify({"status": "success", "message": "Data queued"})
        except Exception as e:
            logger.error(f"[SENSOR ROUTE ERROR] {e}", exc_info=True)
            return jsonify({"status": "error", "message": "Internal server error"}), 500

    # NOTE: /stats endpoint is now in system_routes.py to avoid duplication