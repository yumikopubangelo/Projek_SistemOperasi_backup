from .sensor_routes import register_sensor_routes
from .ml_routes import register_ml_routes
from .system_routes import register_system_routes
from .dashboard_routes import register_dashboard_routes


def register_routes(app, services):
    """Register all route modules with dependency injection."""
    register_sensor_routes(app, services)
    register_ml_routes(app, services)
    register_system_routes(app, services)
    register_dashboard_routes(app, services)
