from flask import Blueprint, request, jsonify
import logging
from datetime import datetime, timedelta
from core.config import Config
from prediction_engine import WaterQualityPredictor

logger = logging.getLogger(__name__)

def create_prediction_blueprint(es_client):
    prediction_bp = Blueprint('prediction', __name__, url_prefix='/prediction')
    predictor = WaterQualityPredictor()

    @prediction_bp.route('/filter-rul', methods=['GET'])
    def get_filter_rul():
        try:
            hours_back = int(request.args.get('hours_back', 168))
            initial_tds = float(request.args.get('initial_tds', 50))
            critical_tds = float(request.args.get('critical_tds', 700))

            result = es_client.search(
                index=Config.ELASTIC_INDEX,
                size=min(hours_back * 12, 1000),
                sort=[{"@timestamp": {"order": "desc"}}],
                query={"range": {"@timestamp": {"gte": f"now-{hours_back}h"}}},
                _source_includes=["tds_ppm", "@timestamp"],
                request_timeout=5
            )

            if result['hits']['total']['value'] == 0:
                return jsonify({
                    'status': 'insufficient_data',
                    'message': 'No historical data available'
                }), 200

            historical_data = [hit['_source'] for hit in reversed(result['hits']['hits'])]

            rul_result = predictor.calculate_filter_rul(
                historical_data,
                parameter='tds_ppm',
                initial_tds=initial_tds,
                critical_tds=critical_tds
            )

            return jsonify(rul_result), 200

        except Exception as e:
            logger.error(f"[PREDICTION ERROR] Filter RUL: {e}", exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @prediction_bp.route("/time-to-threshold", methods=["GET"])
    def get_time_to_threshold():
        """Predict time until parameter reaches threshold"""
        try:
            # Get parameters
            parameter = request.args.get("parameter", "tds_ppm")
            threshold = float(request.args.get("threshold", 700))
            hours_back = int(request.args.get("hours_back", 48))

            # Fetch historical data
            result = es_client.search(
                index=Config.ELASTIC_INDEX,
                size=min(hours_back * 12, 1000),
                sort=[{"@timestamp": {"order": "desc"}}],
                body={
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": f"now-{hours_back}h"
                            }
                        }
                    },
                    "_source": [parameter, "@timestamp"]
                }
            )

            if result["hits"]["total"]["value"] == 0:
                return jsonify({
                    "status": "error",
                    "message": "No historical data available",
                }), 404

            # Convert to list
            historical_data = [hit["_source"] for hit in reversed(result["hits"]["hits"])]

            # Predict time to threshold
            prediction = predictor.predict_time_to_threshold(
                historical_data,
                parameter=parameter,
                threshold=threshold,
            )

            logger.info(
                f"[PREDICTION] Time to threshold: "
                f"{prediction.get('estimated_hours', 'N/A')} hours"
            )

            return jsonify(prediction), 200

        except Exception as e:
            logger.error(f"[PREDICTION ERROR] Time to threshold: {e}", exc_info=True)
            return jsonify({
                "status": "error",
                "message": str(e),
            }), 500

    @prediction_bp.route("/next-value", methods=["GET"])
    def get_next_value_prediction():
        """Predict next value of a parameter"""
        try:
            # Get parameters
            parameter = request.args.get("parameter", "tds_ppm")
            hours_ahead = int(request.args.get("hours_ahead", 1))
            method = request.args.get("method", "linear")
            hours_back = int(request.args.get("hours_back", 24))

            # Fetch historical data
            result = es_client.search(
                index=Config.ELASTIC_INDEX,
                size=min(hours_back * 12, 500),
                sort=[{"@timestamp": {"order": "desc"}}],
                body={
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": f"now-{hours_back}h"
                            }
                        }
                    },
                    "_source": [parameter, "@timestamp"]
                }
            )

            if result["hits"]["total"]["value"] == 0:
                return jsonify({
                    "status": "error",
                    "message": "No historical data available",
                }), 404

            # Convert to list
            historical_data = [hit["_source"] for hit in reversed(result["hits"]["hits"])]

            # Predict next value
            prediction = predictor.predict_next_value(
                historical_data,
                parameter=parameter,
                hours_ahead=hours_ahead,
                method=method,
            )

            logger.info(
                f"[PREDICTION] Next value ({method}): "
                f"{prediction.get('predicted_value', 'N/A')}"
            )

            return jsonify(prediction), 200

        except Exception as e:
            logger.error(f"[PREDICTION ERROR] Next value: {e}", exc_info=True)
            return jsonify({
                "status": "error",
                "message": str(e),
            }), 500

    @prediction_bp.route("/forecast", methods=["GET"])
    def get_forecast():
        """Get detailed forecast with multiple time points"""
        try:
            # Get parameters
            parameter = request.args.get("parameter", "tds_ppm")
            hours_ahead = int(request.args.get("hours_ahead", 24))
            intervals = int(request.args.get("intervals", 6))  # Number of forecast points
            method = request.args.get("method", "linear")
            hours_back = int(request.args.get("hours_back", 48))

            # Fetch historical data
            result = es_client.search(
                index=Config.ELASTIC_INDEX,
                size=min(hours_back * 12, 500),
                sort=[{"@timestamp": {"order": "desc"}}],
                body={
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": f"now-{hours_back}h"
                            }
                        }
                    },
                    "_source": [parameter, "@timestamp"]
                }
            )

            if result["hits"]["total"]["value"] == 0:
                return jsonify({
                    "status": "error",
                    "message": "No historical data available",
                }), 404

            # Convert to list
            historical_data = [hit["_source"] for hit in reversed(result["hits"]["hits"])]

            # Generate forecast points
            forecast_points = []
            interval_hours = hours_ahead / intervals

            for i in range(1, intervals + 1):
                hours = interval_hours * i
                prediction = predictor.predict_next_value(
                    historical_data,
                    parameter=parameter,
                    hours_ahead=hours,
                    method=method,
                )

                if prediction["status"] == "success":
                    forecast_time = datetime.now() + timedelta(hours=hours)
                    forecast_points.append({
                        "timestamp": forecast_time.isoformat(),
                        "hours_ahead": hours,
                        "predicted_value": prediction["predicted_value"],
                        "lower_bound": prediction["lower_bound"],
                        "upper_bound": prediction["upper_bound"],
                    })

            logger.info(
                f"[PREDICTION] Forecast generated: {len(forecast_points)} points"
            )

            return jsonify({
                "status": "success",
                "parameter": parameter,
                "method": method,
                "forecast": forecast_points,
            }), 200

        except Exception as e:
            logger.error(f"[PREDICTION ERROR] Forecast: {e}", exc_info=True)
            return jsonify({
                "status": "error",
                "message": str(e),
            }), 500

    @prediction_bp.route("/summary", methods=["GET"])
    def get_prediction_summary():
        """Get comprehensive prediction summary"""
        try:
            hours_back = int(request.args.get("hours_back", 168))

            # Fetch historical data for both TDS and Turbidity
            result = es_client.search(
                index=Config.ELASTIC_INDEX,
                size=min(hours_back * 12, 1000),
                sort=[{"@timestamp": {"order": "desc"}}],
                body={
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": f"now-{hours_back}h"
                            }
                        }
                    },
                    "_source": ["tds_ppm", "kekeruhan_ntu", "@timestamp"]
                }
            )

            if result["hits"]["total"]["value"] == 0:
                return jsonify({
                    "status": "error",
                    "message": "No historical data available",
                }), 404

            historical_data = [hit["_source"] for hit in reversed(result["hits"]["hits"])]

            # Calculate all predictions
            summary = {
                "status": "success",
                "data_points": len(historical_data),
                "time_range_hours": hours_back,
            }

            # Filter RUL
            try:
                rul = predictor.calculate_filter_rul(historical_data)
                summary["filter_rul"] = rul
            except Exception as e:
                logger.warning(f"[PREDICTION] RUL calculation failed: {e}")
                summary["filter_rul"] = {
                    "status": "error",
                    "message": str(e),
                }

            # Time to TDS threshold
            try:
                tds_threshold = predictor.predict_time_to_threshold(
                    historical_data,
                    parameter="tds_ppm",
                    threshold=700,
                )
                summary["tds_threshold"] = tds_threshold
            except Exception as e:
                logger.warning(f"[PREDICTION] TDS threshold failed: {e}")
                summary["tds_threshold"] = {
                    "status": "error",
                    "message": str(e),
                }

            # Time to Turbidity threshold
            try:
                turb_threshold = predictor.predict_time_to_threshold(
                    historical_data,
                    parameter="kekeruhan_ntu",
                    threshold=10.0,
                )
                summary["turbidity_threshold"] = turb_threshold
            except Exception as e:
                logger.warning(f"[PREDICTION] Turbidity threshold failed: {e}")
                summary["turbidity_threshold"] = {
                    "status": "error",
                    "message": str(e),
                }

            logger.info("[PREDICTION] Summary generated successfully")

            return jsonify(summary), 200

        except Exception as e:
            logger.error(f"[PREDICTION ERROR] Summary: {e}", exc_info=True)
            return jsonify({
                "status": "error",
                "message": "Failed to compute prediction summary. Please contact support if the issue persists.",
            }), 500

    return prediction_bp