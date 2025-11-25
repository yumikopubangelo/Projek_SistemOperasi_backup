import os
from flask import jsonify

def register_dashboard_routes(app, services):
    @app.route("/", methods=["GET"])
    def dashboard():
        script_dir = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(script_dir)  # go back to src/
        ml_dashboard = os.path.join(root, "dashboard_ml.html")
        dashboard = os.path.join(root, "dashboard.html")

        if os.path.exists(ml_dashboard):
            return open(ml_dashboard, encoding="utf-8").read()
        if os.path.exists(dashboard):
            return open(dashboard, encoding="utf-8").read()

        return jsonify({"status": "error", "message": "dashboard not found"}), 404
