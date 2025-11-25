"""
AquaGuard Middleware Server v6.0
Main Application Entry Point
"""

import logging
import sys
import signal
from flask import Flask
from flask_cors import CORS
from waitress import serve

from core.config import Config
from core.es_client import ElasticsearchClient
from services.buffering_service import BufferingService
from services.alert_service import AlertService
from services.ml_service import MLService
from routes import register_routes  # ← INI SUDAH INCLUDE SEMUA ROUTES
from routes.prediction_routes import create_prediction_blueprint
from csv_exporter import CSVExporter

# JANGAN IMPORT INDIVIDUAL ROUTES LAGI:
# from routes.sensor_routes import register_sensor_routes  ← HAPUS
# from routes.ml_routes import register_ml_routes          ← HAPUS
# from routes.system_routes import register_system_routes  ← HAPUS
# from routes.dashboard_routes import register_dashboard_routes ← HAPUS


# Validate configuration
try:
    Config.validate()
except (ValueError, FileNotFoundError) as e:
    print(f"[FATAL] Configuration error: {e}")
    sys.exit(1)

# Setup logging
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

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# ==================== INITIALIZATION ====================
def initialize_services():
    """Initialize all services"""
    logger.info("=" * 70)
    logger.info("AQUAGUARD MIDDLEWARE SERVER v6.0")
    logger.info("=" * 70)
    
    # Initialize Elasticsearch
    es_client = ElasticsearchClient()
    es = es_client.client

    # Initialize CSV Exporter (optional)
    csv_exporter = None
    if Config.CSV_EXPORT_ENABLED:
        try:
            csv_exporter = CSVExporter(export_interval=Config.CSV_EXPORT_INTERVAL)
            logger.info("[INIT] CSV Exporter enabled")
            logger.info(f"[INFO] Reports will be saved to: {csv_exporter.output_dir}")
        except Exception as e:
            logger.warning(f"[WARNING] CSV Exporter disabled: {e}")
    
    # Initialize Buffer Manager
    buffer_manager = BufferingService(
        es_client=es,
        csv_exporter=csv_exporter
    )
    
    # Initialize Telegram Notifier
    telegram_notifier = None
    try:
        telegram_notifier = AlertService(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            csv_exporter=csv_exporter
        )
    except Exception as e:
        logger.warning(f"[WARNING] Telegram notifier disabled: {e}")
    
    # Initialize ML Service
    ml_service = MLService(es, Config)

    
    # Initialize ML Service
    ml_service = MLService(es_client=es, config=Config)

    return {
        'es': es,
        'buffer_manager': buffer_manager,
        'telegram_notifier': telegram_notifier,
        'ml_service': ml_service,
        'csv_exporter': csv_exporter,
        'config': Config,   # sekalian tambahin ini biar system_routes bisa pakai
    }


# Initialize services
services = initialize_services()

# ==================== REGISTER ROUTES ====================
# HANYA GUNAKAN register_routes() - ini sudah include semua routes
register_routes(app, services)

# HAPUS BARIS-BARIS INI (duplicate registration):
# register_sensor_routes(app, services)     ← HAPUS
# register_ml_routes(app, services)         ← HAPUS
# register_system_routes(app, services)     ← HAPUS
# register_dashboard_routes(app, services)  ← HAPUS

# Register prediction blueprint
prediction_bp = create_prediction_blueprint(services['es'])
app.register_blueprint(prediction_bp)

# ==================== GRACEFUL SHUTDOWN ====================
def graceful_shutdown(signum, frame):
    """Handle graceful shutdown"""
    logger.info("[SHUTDOWN] Graceful shutdown initiated...")
    services['buffer_manager'].force_flush()
    logger.info("[SHUTDOWN] Complete")
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

# ==================== RUN SERVER ====================
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info(f"[START] Server starting on {Config.SERVER_HOST}:{Config.SERVER_PORT}")
    logger.info("=" * 70)
    
    serve(
        app,
        host=Config.SERVER_HOST,
        port=Config.SERVER_PORT,
        threads=Config.SERVER_THREADS
    )