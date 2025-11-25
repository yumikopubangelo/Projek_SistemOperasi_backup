"""
Alert Service
Smart anomaly detection and Telegram notification system with CSV logging
"""

import logging
import time
import requests
from datetime import datetime, timezone, timedelta
from threading import Thread, Lock
from collections import deque
from typing import Dict, Any, Optional, List

from core import Config


logger = logging.getLogger(__name__)


class AlertService:
    """
    Intelligent alert service with anomaly detection and Telegram notifications
    
    Features:
    - Real-time anomaly detection with severity classification
    - Smart alert aggregation (prevents spam)
    - Configurable cooldown periodsx
    - Telegram integration
    - CSV logging support
    - Detailed analytics
    """
    
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        csv_exporter: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize alert service
        
        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID
            csv_exporter: Optional CSV exporter instance
            config: Optional custom configuration (uses Config if None)
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.csv_exporter = csv_exporter
        
        # Load configuration
        self.min_anomalies_for_alert = Config.ALERT_MIN_ANOMALIES
        self.time_window_hours = Config.ALERT_TIME_WINDOW_HOURS
        self.cooldown_minutes = Config.ALERT_COOLDOWN_MINUTES

        
        # Anomaly buffer
        self.anomaly_buffer = deque(maxlen=100)
        self.lock = Lock()
        
        # Statistics
        self.last_alert_time: Optional[datetime] = None
        self.total_alerts_sent = 0
        self.total_anomalies_detected = 0
        self.alerts_suppressed = 0
        
        # Start monitoring thread
        self._running = True
        self._start_monitor()
        
        logger.info("=" * 70)
        logger.info("ALERT SERVICE INITIALIZED")
        logger.info("=" * 70)
        logger.info(f"Alert threshold: {self.min_anomalies_for_alert} anomalies/{self.time_window_hours}h")
        logger.info(f"Cooldown period: {self.cooldown_minutes} minutes")
        logger.info(f"CSV Export: {'Enabled' if csv_exporter else 'Disabled'}")
        logger.info("=" * 70)
        
        # Send startup notification
        self._send_startup_notification()
    
    def _start_monitor(self) -> None:
        """Start background monitoring thread"""
        self.monitor_thread = Thread(
            target=self._monitor_worker,
            name="AlertMonitorWorker",
            daemon=True
        )
        self.monitor_thread.start()
        logger.info("[OK] Alert monitoring worker started")

    
    def _send_startup_notification(self) -> None:
        """Send system startup notification"""
        message = (
            "🟢 SYSTEM STARTED\n\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Status: Anomaly Detection Active\n"
            f"Alert threshold: {self.min_anomalies_for_alert} anomalies/{self.time_window_hours}h\n"
            f"Cooldown: {self.cooldown_minutes} minutes"
        )
        self._send_telegram_message(message)
    
    def record_anomaly(
        self,
        data_type: str,
        value: float,
        threshold: float,
        severity: str = "medium"
    ) -> None:
        """
        Record an anomaly detection
        
        Args:
            data_type: Type of data (e.g., "TDS", "Turbidity")
            value: Measured value
            threshold: Threshold that was exceeded
            severity: Severity level ("low", "medium", "high")
        """
        with self.lock:
            anomaly = {
                'timestamp': datetime.now(timezone.utc),
                'type': data_type,
                'value': value,
                'threshold': threshold,
                'severity': severity
            }
            self.anomaly_buffer.append(anomaly)
            self.total_anomalies_detected += 1
            
            logger.warning(
                f"🔴 ANOMALY: {data_type}={value:.2f} "
                f"(threshold: {threshold}, severity: {severity.upper()})"
            )
            
            # Log to CSV
            if self.csv_exporter:
                self.csv_exporter.log_anomaly(data_type, value, threshold, severity)
    
    def _count_recent_anomalies(self) -> tuple[int, List[Dict[str, Any]]]:
        """
        Count anomalies within time window
        
        Returns:
            tuple: (count, list of recent anomalies)
        """
        with self.lock:
            if not self.anomaly_buffer:
                return 0, []
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.time_window_hours)
            recent = [a for a in self.anomaly_buffer if a['timestamp'] > cutoff_time]
            return len(recent), recent
    
    def _should_send_alert(self) -> bool:
        """
        Determine if alert should be sent based on rules
        
        Returns:
            bool: True if alert should be sent
        """
        count, _ = self._count_recent_anomalies()
        
        # Check minimum threshold
        if count < self.min_anomalies_for_alert:
            return False
        
        # Check cooldown period
        if self.last_alert_time:
            time_since = datetime.now(timezone.utc) - self.last_alert_time
            if time_since < timedelta(minutes=self.cooldown_minutes):
                self.alerts_suppressed += 1
                logger.debug(
                    f"Alert suppressed (cooldown active, "
                    f"{self.cooldown_minutes - time_since.seconds // 60} min remaining)"
                )
                return False
        
        return True
    
    def _analyze_anomalies(self, anomalies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze anomalies to generate summary
        
        Args:
            anomalies: List of anomaly records
        
        Returns:
            dict: Analysis results
        """
        if not anomalies:
            return {}
        
        # Group by type
        tds = [a for a in anomalies if a['type'] == 'TDS']
        turbidity = [a for a in anomalies if a['type'] == 'Turbidity']
        
        # Count by severity
        high = len([a for a in anomalies if a['severity'] == 'high'])
        medium = len([a for a in anomalies if a['severity'] == 'medium'])
        low = len([a for a in anomalies if a['severity'] == 'low'])
        
        return {
            'total': len(anomalies),
            'tds_count': len(tds),
            'turbidity_count': len(turbidity),
            'high_severity': high,
            'medium_severity': medium,
            'low_severity': low,
            'avg_tds': sum(a['value'] for a in tds) / len(tds) if tds else 0,
            'avg_turbidity': sum(a['value'] for a in turbidity) / len(turbidity) if turbidity else 0,
            'first_detected': anomalies[0]['timestamp'],
            'last_detected': anomalies[-1]['timestamp']
        }
    
    def _format_alert_message(self, analysis: Dict[str, Any]) -> str:
        """
        Format alert message for Telegram
        
        Args:
            analysis: Anomaly analysis results
        
        Returns:
            str: Formatted message
        """
        # Determine priority
        if analysis['high_severity'] > 0:
            priority = "🔴 HIGH PRIORITY"
        elif analysis['medium_severity'] >= 3:
            priority = "🟡 MEDIUM PRIORITY"
        else:
            priority = "🟢 LOW PRIORITY"
        
        message = f"⚠️ WATER QUALITY ALERT\n\n"
        message += f"Priority: {priority}\n"
        message += f"Anomalies: {analysis['total']} in {self.time_window_hours}h\n\n"
        
        message += "📊 Breakdown:\n"
        if analysis['tds_count'] > 0:
            message += f"• TDS: {analysis['tds_count']} (avg: {analysis['avg_tds']:.1f} ppm)\n"
        if analysis['turbidity_count'] > 0:
            message += f"• Turbidity: {analysis['turbidity_count']} (avg: {analysis['avg_turbidity']:.1f} NTU)\n"
        
        message += f"\n🎯 Severity:\n"
        message += f"• High: {analysis['high_severity']}\n"
        message += f"• Medium: {analysis['medium_severity']}\n"
        message += f"• Low: {analysis['low_severity']}\n"
        
        # Duration
        duration = (analysis['last_detected'] - analysis['first_detected']).total_seconds() / 60
        message += f"\n⏱ Duration: {duration:.0f} min\n"
        message += f"🕒 Latest: {analysis['last_detected'].strftime('%H:%M:%S')}"
        
        return message
    
    def _monitor_worker(self) -> None:
        """Background worker for anomaly monitoring"""
        while self._running:
            try:
                time.sleep(60)  # Check every minute
                
                if self._should_send_alert():
                    count, recent = self._count_recent_anomalies()
                    
                    if count >= self.min_anomalies_for_alert:
                        analysis = self._analyze_anomalies(recent)
                        message = self._format_alert_message(analysis)
                        
                        if self._send_telegram_message(message):
                            self.last_alert_time = datetime.now(timezone.utc)
                            self.total_alerts_sent += 1
                            logger.info(
                                f"✓ Telegram alert sent (total: {self.total_alerts_sent})"
                            )
            
            except Exception as e:
                logger.error(f"Monitor worker error: {e}", exc_info=True)
    
    def _send_telegram_message(self, message: str) -> bool:
        """
        Send message to Telegram
        
        Args:
            message: Message text
        
        Returns:
            bool: True if successful
        """
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.debug("Telegram message sent successfully")
                return True
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return False
        
        except requests.exceptions.Timeout:
            logger.error("Telegram request timeout")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Telegram request error: {e}")
            return False
        except Exception as e:
            logger.error(f"Telegram send error: {e}", exc_info=True)
            return False
    
    def send_custom_alert(self, message: str) -> bool:
        """
        Send custom alert message
        
        Args:
            message: Custom message text
        
        Returns:
            bool: True if successful
        """
        return self._send_telegram_message(message)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get alert service statistics
        
        Returns:
            dict: Statistics information
        """
        with self.lock:
            count, _ = self._count_recent_anomalies()
            return {
                'total_anomalies': self.total_anomalies_detected,
                'recent_count': count,
                'time_window_hours': self.time_window_hours,
                'alerts_sent': self.total_alerts_sent,
                'alerts_suppressed': self.alerts_suppressed,
                'last_alert': self.last_alert_time.isoformat() if self.last_alert_time else None,
                'cooldown_minutes': self.cooldown_minutes
            }
    
    def shutdown(self) -> None:
        """Gracefully shutdown the service"""
        logger.info("Shutting down alert service...")
        self._running = False
        
        # Wait for monitor thread to finish
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        # Send shutdown notification
        message = (
            "🔴 SYSTEM SHUTDOWN\n\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Total alerts sent: {self.total_alerts_sent}\n"
            f"Total anomalies: {self.total_anomalies_detected}"
        )
        self._send_telegram_message(message)
        
        logger.info("✓ Alert service shutdown complete")
    
    def __repr__(self) -> str:
        return (
            f"<AlertService "
            f"anomalies={self.total_anomalies_detected} "
            f"alerts={self.total_alerts_sent}>"
        )