import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class Config:
    """Configuration class loaded from .env file"""
    
    # Elasticsearch Configuration
    ELASTIC_HOST = os.getenv("ELASTIC_HOST", "https://localhost:9200")
    ELASTIC_USER = os.getenv("ELASTIC_USER", "elastic")
    ELASTIC_PASS = os.getenv("ELASTIC_PASS")
    ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "depot_air_qc_data")
    ELASTIC_CA_CERT = os.getenv("ELASTIC_CA_CERT", "http_ca.crt")
    
    # Server Configuration
    SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
    SERVER_PORT = int(os.getenv("SERVER_PORT", "5000"))
    SERVER_THREADS = int(os.getenv("SERVER_THREADS", "4"))
    SECRET_KEY = os.getenv("SECRET_KEY")
    
    # Telegram Configuration
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    # Anomaly Detection Thresholds
    TDS_HIGH_THRESHOLD = float(os.getenv("TDS_HIGH_THRESHOLD", "500"))
    TDS_CRITICAL_THRESHOLD = float(os.getenv("TDS_CRITICAL_THRESHOLD", "700"))
    TURBIDITY_HIGH_THRESHOLD = float(os.getenv("TURBIDITY_HIGH_THRESHOLD", "5.0"))
    TURBIDITY_CRITICAL_THRESHOLD = float(os.getenv("TURBIDITY_CRITICAL_THRESHOLD", "10.0"))
    
    # Adaptive Buffer Configuration
    ADAPTIVE_MIN_BUFFER = int(os.getenv("ADAPTIVE_MIN_BUFFER", "5"))
    ADAPTIVE_MAX_BUFFER = int(os.getenv("ADAPTIVE_MAX_BUFFER", "100"))
    ADAPTIVE_MIN_INTERVAL = float(os.getenv("ADAPTIVE_MIN_INTERVAL", "0.5"))
    ADAPTIVE_MAX_INTERVAL = float(os.getenv("ADAPTIVE_MAX_INTERVAL", "5.0"))
    
    # Alert Configuration
    ALERT_MIN_ANOMALIES = int(os.getenv("ALERT_MIN_ANOMALIES", "5"))
    ALERT_TIME_WINDOW_HOURS = int(os.getenv("ALERT_TIME_WINDOW_HOURS", "1"))
    ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "30"))
    
    # CSV Export Configuration
    CSV_EXPORT_ENABLED = os.getenv("CSV_EXPORT_ENABLED", "true").lower() == "true"
    CSV_EXPORT_INTERVAL = int(os.getenv("CSV_EXPORT_INTERVAL", "3600"))
    CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR", "./reports")
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "aquaguard.log")
    
    # ML Job Names (optional)
    ML_JOB_IDS = ["anomali_kekeruhan", "prediksi_tds_jenuh"]

    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        
        # Check required fields
        if not cls.ELASTIC_PASS:
            errors.append("ELASTIC_PASS is required")
        
        if not cls.SECRET_KEY:
            errors.append("SECRET_KEY is required")
        
        # Check CA certificate file
        if not os.path.exists(cls.ELASTIC_CA_CERT):
            errors.append(f"CA certificate not found: {cls.ELASTIC_CA_CERT}")
        
        # Validate numeric ranges
        if cls.ADAPTIVE_MIN_BUFFER >= cls.ADAPTIVE_MAX_BUFFER:
            errors.append("ADAPTIVE_MIN_BUFFER must be less than ADAPTIVE_MAX_BUFFER")
        
        if cls.ADAPTIVE_MIN_INTERVAL >= cls.ADAPTIVE_MAX_INTERVAL:
            errors.append("ADAPTIVE_MIN_INTERVAL must be less than ADAPTIVE_MAX_INTERVAL")
        
        if cls.TDS_HIGH_THRESHOLD >= cls.TDS_CRITICAL_THRESHOLD:
            errors.append("TDS_HIGH_THRESHOLD must be less than TDS_CRITICAL_THRESHOLD")
        
        if cls.TURBIDITY_HIGH_THRESHOLD >= cls.TURBIDITY_CRITICAL_THRESHOLD:
            errors.append("TURBIDITY_HIGH_THRESHOLD must be less than TURBIDITY_CRITICAL_THRESHOLD")
        
        # Create CSV output directory if needed
        if cls.CSV_EXPORT_ENABLED:
            Path(cls.CSV_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        
        if errors:
            raise ValueError("Configuration errors:\n- " + "\n- ".join(errors))
        
        return True
    
    @classmethod
    def print_config(cls):
        """Print current configuration (hide sensitive data)"""
        print("\n" + "=" * 70)
        print("CONFIGURATION SUMMARY")
        print("=" * 70)
        print(f"Elasticsearch Host: {cls.ELASTIC_HOST}")
        print(f"Elasticsearch Index: {cls.ELASTIC_INDEX}")
        print(f"Server: {cls.SERVER_HOST}:{cls.SERVER_PORT}")
        print(f"Adaptive Buffer: {cls.ADAPTIVE_MIN_BUFFER}-{cls.ADAPTIVE_MAX_BUFFER} docs")
        print(f"Adaptive Interval: {cls.ADAPTIVE_MIN_INTERVAL}-{cls.ADAPTIVE_MAX_INTERVAL}s")
        print(f"TDS Thresholds: {cls.TDS_HIGH_THRESHOLD}/{cls.TDS_CRITICAL_THRESHOLD} ppm")
        print(f"Turbidity Thresholds: {cls.TURBIDITY_HIGH_THRESHOLD}/{cls.TURBIDITY_CRITICAL_THRESHOLD} NTU")
        print(f"CSV Export: {'Enabled' if cls.CSV_EXPORT_ENABLED else 'Disabled'}")
        if cls.CSV_EXPORT_ENABLED:
            print(f"CSV Output Dir: {cls.CSV_OUTPUT_DIR}")
        print(f"Telegram Alerts: {'Enabled' if cls.TELEGRAM_BOT_TOKEN else 'Disabled'}")
        print(f"Log Level: {cls.LOG_LEVEL}")
        print("=" * 70 + "\n")
        
@staticmethod
def get_alert_config():
    return {
        "min_anomalies": Config.ALERT_MIN_ANOMALIES,
        "time_window_hours": Config.ALERT_TIME_WINDOW_HOURS,
        "cooldown_minutes": Config.ALERT_COOLDOWN_MINUTES,
        "bot_token": Config.TELEGRAM_BOT_TOKEN,
        "chat_id": Config.TELEGRAM_CHAT_ID,
    }
