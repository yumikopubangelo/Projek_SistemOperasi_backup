"""
Adaptive Buffering Service
Self-tuning bulk buffer manager for Elasticsearch with CSV export support
"""

import logging
import time
from datetime import datetime, timezone
from threading import Thread, Lock
from collections import deque
from typing import Dict, Any, Optional, Callable

from core.config import Config


logger = logging.getLogger(__name__)


class BufferingService:
    """
    Adaptive bulk buffer manager with dynamic tuning based on traffic patterns
    
    Features:
    - Self-adjusting buffer size and flush interval
    - Traffic pattern detection (LOW/MODERATE/HIGH/BURST)
    - Automatic flush on buffer full or timeout
    - CSV export integration
    - Thread-safe operations
    """
    
    def __init__(
        self,
        es_client,
        csv_exporter: Optional[Any] = None,
        on_flush_callback: Optional[Callable] = None
    ):
        """
        Initialize buffering service
        
        Args:
            es_client: Elasticsearch client instance
            csv_exporter: Optional CSV exporter instance
            on_flush_callback: Optional callback function called after flush
        """
        self.es_client = es_client
        self.csv_exporter = csv_exporter
        self.on_flush_callback = on_flush_callback
        
        # Buffer configuration
        self.buffer = deque()
        self.lock = Lock()
        
        # Adaptive parameters (will be tuned automatically)
        self.buffer_size = Config.ADAPTIVE_MIN_BUFFER
        self.flush_interval = Config.ADAPTIVE_MIN_INTERVAL
        
        # Traffic monitoring
        self.request_timestamps = deque(maxlen=100)
        self.flush_times = deque(maxlen=20)
        
        # Statistics
        self.last_flush_time = time.time()
        self.total_flushed = 0
        self.total_received = 0
        self.adaptation_count = 0
        self.current_strategy = "INITIALIZING"
        
        # Start background threads
        self._running = True
        self._start_threads()
        
        logger.info("=" * 70)
        logger.info("ADAPTIVE BUFFERING SERVICE INITIALIZED")
        logger.info("=" * 70)
        logger.info(f"Buffer range: {Config.ADAPTIVE_MIN_BUFFER}-{Config.ADAPTIVE_MAX_BUFFER} docs")
        logger.info(f"Interval range: {Config.ADAPTIVE_MIN_INTERVAL}-{Config.ADAPTIVE_MAX_INTERVAL}s")
        logger.info(f"CSV Export: {'Enabled' if csv_exporter else 'Disabled'}")
        logger.info("=" * 70)
    
    def _start_threads(self) -> None:
        """Start background worker threads"""
        self.flush_thread = Thread(
            target=self._periodic_flush_worker,
            name="BufferFlushWorker",
            daemon=True
        )
        self.flush_thread.start()
        
        self.adapt_thread = Thread(
            target=self._adaptive_tuning_worker,
            name="BufferAdaptWorker",
            daemon=True
        )
        self.adapt_thread.start()
        
        logger.info("[OK] Background workers started")
    
    def add(self, document: Dict[str, Any]) -> None:
        """
        Add document to buffer (thread-safe)
        
        Args:
            document: Document to buffer
        """
        with self.lock:
            current_time = time.time()
            self.request_timestamps.append(current_time)
            self.total_received += 1
            
            # Add timestamp if not present
            if '@timestamp' not in document:
                document['@timestamp'] = datetime.now(timezone.utc).isoformat()
            
            # Handle field name inconsistency (backward compatibility)
            if 'suhu_celcius' in document and 'suhu_celsius' not in document:
                document['suhu_celsius'] = document['suhu_celcius']
            
            self.buffer.append(document)
            
            # Flush if buffer is full
            if len(self.buffer) >= self.buffer_size:
                logger.debug(f"Buffer full ({len(self.buffer)}/{self.buffer_size}), flushing...")
                self._flush()
    
    def _calculate_traffic_rate(self) -> float:
        """
        Calculate current traffic rate in requests per minute
        
        Returns:
            float: Requests per minute
        """
        if len(self.request_timestamps) < 2:
            return 0.0
        
        time_span = self.request_timestamps[-1] - self.request_timestamps[0]
        if time_span == 0:
            return 0.0
        
        rpm = (len(self.request_timestamps) / time_span) * 60
        return rpm
    
    def _calculate_avg_flush_time(self) -> float:
        """
        Calculate average flush operation time
        
        Returns:
            float: Average flush time in seconds
        """
        if not self.flush_times:
            return 0.5
        return sum(self.flush_times) / len(self.flush_times)
    
    def _determine_strategy(self, traffic_rpm: float) -> tuple:
        """
        Determine optimal buffer strategy based on traffic
        
        Args:
            traffic_rpm: Current traffic rate (requests/min)
        
        Returns:
            tuple: (strategy_name, target_buffer_size, target_flush_interval)
        """
        if traffic_rpm < 5:
            return (
                "LOW_TRAFFIC",
                Config.ADAPTIVE_MIN_BUFFER,
                Config.ADAPTIVE_MIN_INTERVAL
            )
        elif traffic_rpm < 30:
            ratio = (traffic_rpm - 5) / 25
            buffer = int(Config.ADAPTIVE_MIN_BUFFER + (20 - Config.ADAPTIVE_MIN_BUFFER) * ratio)
            interval = Config.ADAPTIVE_MIN_INTERVAL + (2.0 - Config.ADAPTIVE_MIN_INTERVAL) * ratio
            return ("MODERATE_TRAFFIC", buffer, interval)
        elif traffic_rpm < 100:
            ratio = (traffic_rpm - 30) / 70
            buffer = int(20 + (50 - 20) * ratio)
            interval = 2.0 + (3.0 - 2.0) * ratio
            return ("HIGH_TRAFFIC", buffer, interval)
        else:
            return (
                "BURST_TRAFFIC",
                Config.ADAPTIVE_MAX_BUFFER,
                Config.ADAPTIVE_MAX_INTERVAL
            )
    
    def _adaptive_tuning_worker(self) -> None:
        """Background worker for adaptive parameter tuning"""
        while self._running:
            try:
                time.sleep(10)  # Tune every 10 seconds
                
                with self.lock:
                    traffic_rpm = self._calculate_traffic_rate()
                    avg_flush_time = self._calculate_avg_flush_time()
                    
                    old_buffer_size = self.buffer_size
                    old_flush_interval = self.flush_interval
                    
                    # Determine strategy
                    strategy, target_buffer, target_interval = self._determine_strategy(traffic_rpm)
                    
                    # Adjust for slow flushes
                    if avg_flush_time > 1.0:
                        target_buffer = min(target_buffer * 1.2, Config.ADAPTIVE_MAX_BUFFER)
                    
                    # Smooth transition (exponential moving average)
                    self.buffer_size = int(self.buffer_size * 0.7 + target_buffer * 0.3)
                    self.flush_interval = self.flush_interval * 0.7 + target_interval * 0.3
                    
                    # Clamp to configured limits
                    self.buffer_size = max(
                        Config.ADAPTIVE_MIN_BUFFER,
                        min(Config.ADAPTIVE_MAX_BUFFER, self.buffer_size)
                    )
                    self.flush_interval = max(
                        Config.ADAPTIVE_MIN_INTERVAL,
                        min(Config.ADAPTIVE_MAX_INTERVAL, self.flush_interval)
                    )
                    
                    # Log significant changes
                    buffer_changed = abs(old_buffer_size - self.buffer_size) > 2
                    interval_changed = abs(old_flush_interval - self.flush_interval) > 0.5
                    
                    if buffer_changed or interval_changed:
                        self.adaptation_count += 1
                        self.current_strategy = strategy
                        
                        logger.info(
                            f"[ADAPT #{self.adaptation_count}] {strategy} | "
                            f"Traffic: {traffic_rpm:.1f} req/min | "
                            f"Buffer: {old_buffer_size}->{self.buffer_size} | "
                            f"Interval: {old_flush_interval:.1f}s->{self.flush_interval:.1f}s"
                        )
                        
                        # Export to CSV
                        if self.csv_exporter:
                            stats = self.get_stats()
                            self.csv_exporter.export_adaptive_stats(stats)
            
            except Exception as e:
                logger.error(f"Adaptive tuning error: {e}", exc_info=True)
    
    def _flush(self) -> None:
        """
        Flush buffer to Elasticsearch (NOT thread-safe, must be called with lock)
        """
        if not self.buffer:
            return
        
        flush_start_time = time.time()
        buffer_size = len(self.buffer)
        
        try:
            # Convert buffer to list for bulk operation
            documents = list(self.buffer)
            
            # Bulk index
            success, failed = self.es_client.bulk_index(documents)
            
            self.total_flushed += success
            
            # Record flush time
            flush_duration = time.time() - flush_start_time
            self.flush_times.append(flush_duration)
            
            if failed > 0:
                logger.warning(f"[WARNING] {failed} documents failed to index")
            
            logger.info(
                f"[OK] Flushed {success}/{buffer_size} docs in {flush_duration:.3f}s "
                f"(total: {self.total_flushed:,}, buffer: {self.buffer_size})"
            )
            
            # Clear buffer
            self.buffer.clear()
            self.last_flush_time = time.time()
            
            # Call callback if provided
            if self.on_flush_callback:
                self.on_flush_callback(success, failed, flush_duration)
        
        except Exception as e:
            logger.error(f"Bulk flush error: {e}", exc_info=True)
    
    def _periodic_flush_worker(self) -> None:
        """Background worker for periodic flushing"""
        while self._running:
            try:
                time.sleep(0.5)  # Check every 500ms
                
                with self.lock:
                    time_since_last_flush = time.time() - self.last_flush_time
                    
                    if self.buffer and time_since_last_flush >= self.flush_interval:
                        logger.debug(f"Periodic flush triggered ({len(self.buffer)} docs)")
                        self._flush()
            
            except Exception as e:
                logger.error(f"Periodic flush error: {e}", exc_info=True)
    
    def force_flush(self) -> int:
        """
        Force immediate flush of buffer
        
        Returns:
            int: Number of documents flushed
        """
        with self.lock:
            pending = len(self.buffer)
            if pending > 0:
                logger.info(f"Force flush: {pending} pending documents")
                self._flush()
            return pending
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current buffer statistics
        
        Returns:
            dict: Statistics information
        """
        with self.lock:
            return {
                "timestamp": datetime.now().isoformat(),
                "current_buffer_size": len(self.buffer),
                "current_size": len(self.buffer),  # Alias for dashboard
                "configured_buffer_size": self.buffer_size,
                "configured_flush_interval": round(self.flush_interval, 2),
                "total_received": self.total_received,
                "total_flushed": self.total_flushed,
                "total_sent": self.total_flushed,  # Alias for dashboard
                "pending": self.total_received - self.total_flushed,
                "failed": 0,  # Not tracking failures separately yet
                "total_failed": 0,  # Alias for dashboard
                "traffic_rpm": round(self._calculate_traffic_rate(), 2),
                "avg_flush_time": round(self._calculate_avg_flush_time(), 3),
                "adaptation_count": self.adaptation_count,
                "current_strategy": self.current_strategy,
                "last_flush": datetime.fromtimestamp(self.last_flush_time).isoformat()
            }
    
    def shutdown(self) -> None:
        """Gracefully shutdown the service"""
        logger.info("Shutting down buffering service...")
        self._running = False
        
        # Force final flush
        pending = self.force_flush()
        if pending > 0:
            logger.info(f"[OK] Final flush completed: {pending} documents")
        
        # Wait for threads to finish
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        if self.adapt_thread.is_alive():
            self.adapt_thread.join(timeout=5)
        
        logger.info("[OK] Buffering service shutdown complete")
    
    def __repr__(self) -> str:
        return (
            f"<BufferingService "
            f"buffer={len(self.buffer)}/{self.buffer_size} "
            f"strategy={self.current_strategy}>"
        )