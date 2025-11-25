from flask import request, jsonify
import logging

logger = logging.getLogger(__name__)


def register_ml_routes(app, services):
    ml = services['ml_service']

    @app.route("/ml/anomalies", methods=["GET"])
    def ml_anomalies():
        """
        Get anomalies from all or specific ML job
        Query params:
            - size: number of results (default: 50)
            - min_score: minimum anomaly score (default: 0)
            - hours_back: time range in hours (default: 24)
            - job_id: specific job ID (optional, default: all jobs)
        """
        return jsonify(ml.get_anomalies(
            size=int(request.args.get("size", 50)),
            min_score=float(request.args.get("min_score", 0)),
            hours_back=int(request.args.get("hours_back", 24)),
            job_id=request.args.get("job_id", None)
        ))

    @app.route("/ml/status", methods=["GET"])
    def ml_status():
        """
        Get aggregated status of all ML jobs
        Returns overall status and individual job statuses
        """
        return jsonify(ml.get_status())

    @app.route("/ml/status/<job_id>", methods=["GET"])
    def ml_job_status(job_id):
        """
        Get status of specific ML job
        """
        return jsonify(ml.get_job_status(job_id))

    @app.route("/ml/summary", methods=["GET"])
    def ml_summary():
        """
        Get summary statistics for all ML jobs
        Query params:
            - hours_back: time range in hours (default: 24)
        """
        return jsonify(ml.get_summary(
            hours_back=int(request.args.get("hours_back", 24))
        ))

    @app.route("/ml/health", methods=["GET"])
    def ml_health():
        """
        Quick health check for ML service
        """
        return jsonify(ml.check_health())

    @app.route("/ml/jobs", methods=["GET"])
    def ml_jobs_list():
        """
        Get list of configured ML jobs
        """
        return jsonify({
            "status": "success",
            "jobs": ml.job_ids,
            "total": len(ml.job_ids),
            "default_job": ml.default_job_id
        })