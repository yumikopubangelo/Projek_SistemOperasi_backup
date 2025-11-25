import csv
import os
from datetime import datetime
from pathlib import Path
from threading import Lock

class CSVExporter:
    """CSV Exporter for adaptive stats, sensor data, and anomaly logs"""
    
    def __init__(self, output_dir="./reports", export_interval=3600):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.export_interval = export_interval
        self.lock = Lock()
        
        # File paths based on current date
        self.adaptive_stats_file = None
        self.sensor_data_file = None
        self.anomaly_log_file = None
        
        self._initialize_files()
    
    def _initialize_files(self):
        """Initialize CSV files with headers"""
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Adaptive Stats CSV
        self.adaptive_stats_file = self.output_dir / f"adaptive_stats_{date_str}.csv"
        if not self.adaptive_stats_file.exists():
            with open(self.adaptive_stats_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp',
                    'Current_Buffer_Size',
                    'Configured_Buffer_Size',
                    'Configured_Flush_Interval',
                    'Total_Received',
                    'Total_Flushed',
                    'Pending',
                    'Traffic_RPM',
                    'Avg_Flush_Time',
                    'Adaptation_Count'
                ])
        
        # Sensor Data CSV
        self.sensor_data_file = self.output_dir / f"sensor_data_{date_str}.csv"
        if not self.sensor_data_file.exists():
            with open(self.sensor_data_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp',
                    'TDS_ppm',
                    'Turbidity_NTU',
                    'Temperature_Celsius',
                    'Depot_ID'
                ])
        
        # Anomaly Log CSV
        self.anomaly_log_file = self.output_dir / f"anomaly_log_{date_str}.csv"
        if not self.anomaly_log_file.exists():
            with open(self.anomaly_log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp',
                    'Anomaly_Type',
                    'Value',
                    'Threshold',
                    'Severity',
                    'Exceeded_By'
                ])
    
    def _check_date_rollover(self):
        """Check if date has changed and create new files"""
        date_str = datetime.now().strftime("%Y%m%d")
        current_date = self.adaptive_stats_file.stem.split('_')[-1]
        
        if date_str != current_date:
            self._initialize_files()
    
    def export_adaptive_stats(self, stats):
        """Export adaptive buffer statistics"""
        with self.lock:
            self._check_date_rollover()
            
            try:
                with open(self.adaptive_stats_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        stats.get('timestamp', datetime.now().isoformat()),
                        stats.get('current_buffer_size', 0),
                        stats.get('configured_buffer_size', 0),
                        stats.get('configured_flush_interval', 0),
                        stats.get('total_received', 0),
                        stats.get('total_flushed', 0),
                        stats.get('pending', 0),
                        stats.get('traffic_rpm', 0),
                        stats.get('avg_flush_time', 0),
                        stats.get('adaptation_count', 0)
                    ])
            except Exception as e:
                print(f"[CSV ERROR] Failed to export adaptive stats: {e}")
    
    def log_sensor_data(self, data):
        """Log sensor data to CSV"""
        with self.lock:
            self._check_date_rollover()
            
            try:
                with open(self.sensor_data_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        data.get('@timestamp', datetime.now().isoformat()),
                        data.get('tds_ppm', 0),
                        data.get('kekeruhan_ntu', 0),
                        data.get('suhu_celsius', 0),
                        data.get('depot_id', 'N/A')
                    ])
            except Exception as e:
                print(f"[CSV ERROR] Failed to log sensor data: {e}")
    
    def log_anomaly(self, anomaly_type, value, threshold, severity):
        """Log anomaly detection to CSV"""
        with self.lock:
            self._check_date_rollover()
            
            try:
                exceeded_by = value - threshold
                exceeded_pct = (exceeded_by / threshold) * 100
                
                with open(self.anomaly_log_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        datetime.now().isoformat(),
                        anomaly_type,
                        f"{value:.2f}",
                        f"{threshold:.2f}",
                        severity,
                        f"{exceeded_by:.2f} ({exceeded_pct:.1f}%)"
                    ])
            except Exception as e:
                print(f"[CSV ERROR] Failed to log anomaly: {e}")
    
    def get_file_paths(self):
        """Get current CSV file paths"""
        return {
            'adaptive_stats': str(self.adaptive_stats_file),
            'sensor_data': str(self.sensor_data_file),
            'anomaly_log': str(self.anomaly_log_file)
        }
    
    def get_stats(self):
        """Get CSV export statistics"""
        stats = {}
        
        for name, path in self.get_file_paths().items():
            if os.path.exists(path):
                size = os.path.getsize(path)
                with open(path, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f) - 1  # Exclude header
                
                stats[name] = {
                    'path': path,
                    'size_bytes': size,
                    'size_kb': round(size / 1024, 2),
                    'records': line_count
                }
        
        return stats