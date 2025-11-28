# csv_exporter.py
"""
CSV Exporter for adaptive stats, sensor data, and anomaly logs.
Writes are performed by a single background worker thread to avoid blocking callers.
"""

import csv
import os
import time
import logging
from datetime import datetime
from pathlib import Path
from threading import Thread, Event, RLock
from collections import deque
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class CSVExporter:
    """CSV Exporter for adaptive stats, sensor data, and anomaly logs (background writer)."""

    ADAPTIVE_FIELDS = [
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
    ]

    SENSOR_FIELDS = [
        'Timestamp',
        'TDS_ppm',
        'Turbidity_NTU',
        'Temperature_Celsius',
        'Depot_ID'
    ]

    ANOMALY_FIELDS = [
        'Timestamp',
        'Anomaly_Type',
        'Value',
        'Threshold',
        'Severity',
        'Exceeded_By'
    ]

    def __init__(self, output_dir: str = "./reports", export_interval: int = 3600):
        """
        Args:
            output_dir: directory to store CSV files
            export_interval: (unused here) reserved for periodic batched exports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.lock = RLock()  # protects internal structures (not file writes — worker is single-threaded)
        self._queue = deque()  # stores tuples: (stream_name, row_dict_or_tuple)
        self._evt = Event()
        self._running = True

        # Tracks current open file stems to detect date rollover
        self._current_date = datetime.now().strftime("%Y%m%d")

        # Worker thread
        self._worker = Thread(target=self._worker_loop, name="CSVExporterWorker", daemon=True)
        self._worker.start()
        logger.info(f"CSVExporter initialized -> {self.output_dir}")

    # -------------------------
    # Public API (non-blocking)
    # -------------------------
    def export_adaptive_stats(self, stats: Dict[str, Any]) -> None:
        """Queue adaptive stats for export (non-blocking)."""
        # Normalize row data to tuple consistent with header order
        row = (
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
        )
        self._enqueue('adaptive_stats', row)

    def log_sensor_data(self, data: Dict[str, Any]) -> None:
        """Queue sensor data for export (non-blocking)."""
        row = (
            data.get('@timestamp', datetime.now().isoformat()),
            data.get('tds_ppm', data.get('tds', 0)),
            data.get('kekeruhan_ntu', data.get('turbidity', 0)),
            data.get('suhu_celsius', data.get('suhu_celcius', data.get('temperature_celsius', 0))),
            data.get('depot_id', 'N/A')
        )
        self._enqueue('sensor_data', row)

    def log_anomaly(self, anomaly_type: str, value: float, threshold: float, severity: str) -> None:
        """Queue anomaly record for export (non-blocking)."""
        exceeded_by = value - threshold
        # Avoid division by zero
        if threshold and threshold != 0:
            try:
                exceeded_pct = (exceeded_by / threshold) * 100
            except Exception:
                exceeded_pct = 0.0
        else:
            exceeded_pct = 0.0

        exceeded_str = f"{exceeded_by:.2f} ({exceeded_pct:.1f}%)"
        row = (datetime.now().isoformat(), anomaly_type, f"{value:.2f}", f"{threshold:.2f}", severity, exceeded_str)
        self._enqueue('anomaly_log', row)

    def get_file_paths(self) -> Dict[str, str]:
        """Get current CSV file paths (for the current date)."""
        date_str = self._current_date
        return {
            'adaptive_stats': str(self.output_dir / f"adaptive_stats_{date_str}.csv"),
            'sensor_data': str(self.output_dir / f"sensor_data_{date_str}.csv"),
            'anomaly_log': str(self.output_dir / f"anomaly_log_{date_str}.csv"),
        }

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Return file stats for each CSV (size and record count). This reads from disk."""
        stats = {}
        paths = self.get_file_paths()
        for name, path in paths.items():
            try:
                if os.path.exists(path):
                    size = os.path.getsize(path)
                    # Count lines (exclude header) - read in streaming way
                    with open(path, 'r', encoding='utf-8') as f:
                        # We subtract 1 for header if file not empty
                        lines = sum(1 for _ in f)
                        records = max(0, lines - 1)
                    stats[name] = {
                        'path': path,
                        'size_bytes': size,
                        'size_kb': round(size / 1024, 2),
                        'records': records
                    }
            except Exception as e:
                logger.warning("Failed to get stats for %s: %s", name, e)
        return stats

    def shutdown(self, timeout: float = 5.0) -> None:
        """Stop worker, flush queue, and exit."""
        logger.info("Shutting down CSVExporter (flushing queue)...")
        self._running = False
        self._evt.set()
        self._worker.join(timeout=timeout)
        # If worker still alive, attempt one last flush synchronously
        if self._worker.is_alive():
            logger.warning("CSVExporter worker did not exit in time; performing final flush synchronously.")
            self._drain_queue_sync()

    # -------------------------
    # Internal helpers
    # -------------------------
    def _enqueue(self, stream_name: str, row: Tuple) -> None:
        with self.lock:
            self._queue.append((stream_name, row))
            # wake up worker
            self._evt.set()

    def _worker_loop(self) -> None:
        """Background loop that serially writes queued rows to disk."""
        while self._running or self._queue:
            try:
                # Wait until there is work or shutdown requested
                self._evt.wait(timeout=1.0)
                self._evt.clear()

                # Update date rollover if day changed
                now_date = datetime.now().strftime("%Y%m%d")
                if now_date != self._current_date:
                    logger.info("Date rollover detected: %s -> %s", self._current_date, now_date)
                    self._current_date = now_date

                # Drain queue (batch writes for efficiency)
                batch = []
                with self.lock:
                    while self._queue:
                        batch.append(self._queue.popleft())

                if not batch:
                    continue

                # Group by stream to minimize file opens
                grouped = {}
                for stream_name, row in batch:
                    grouped.setdefault(stream_name, []).append(row)

                for stream_name, rows in grouped.items():
                    try:
                        self._ensure_file_and_header(stream_name)
                        path = self._get_path_for_stream(stream_name)
                        # Append rows
                        with open(path, 'a', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            for r in rows:
                                writer.writerow(r)
                    except Exception as e:
                        logger.exception("Failed to write %s rows to CSV: %s", stream_name, e)

            except Exception as e:
                logger.exception("CSV exporter worker loop error: %s", e)

        logger.info("CSVExporter worker exiting (queue drained).")

    def _drain_queue_sync(self) -> None:
        """Synchronous flush used as a fallback during shutdown."""
        with self.lock:
            items = list(self._queue)
            self._queue.clear()

        if not items:
            return

        grouped = {}
        for stream_name, row in items:
            grouped.setdefault(stream_name, []).append(row)

        for stream_name, rows in grouped.items():
            try:
                self._ensure_file_and_header(stream_name)
                path = self._get_path_for_stream(stream_name)
                with open(path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for r in rows:
                        writer.writerow(r)
            except Exception as e:
                logger.exception("Failed synchronous write for %s: %s", stream_name, e)

    def _get_path_for_stream(self, stream_name: str) -> str:
        date_str = self._current_date
        if stream_name == 'adaptive_stats':
            return str(self.output_dir / f"adaptive_stats_{date_str}.csv")
        elif stream_name == 'sensor_data':
            return str(self.output_dir / f"sensor_data_{date_str}.csv")
        elif stream_name == 'anomaly_log':
            return str(self.output_dir / f"anomaly_log_{date_str}.csv")
        else:
            # Fallback generic file
            return str(self.output_dir / f"{stream_name}_{date_str}.csv")

    def _ensure_file_and_header(self, stream_name: str) -> None:
        """Ensure file exists and header is present (idempotent)."""
        path = Path(self._get_path_for_stream(stream_name))
        # If file doesn't exist, create and write header
        if not path.exists():
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if stream_name == 'adaptive_stats':
                        writer.writerow(self.ADAPTIVE_FIELDS)
                    elif stream_name == 'sensor_data':
                        writer.writerow(self.SENSOR_FIELDS)
                    elif stream_name == 'anomaly_log':
                        writer.writerow(self.ANOMALY_FIELDS)
                    else:
                        # Generic header
                        writer.writerow(['Timestamp', 'Data'])
                logger.debug("Created CSV file with header: %s", path)
            except Exception as e:
                logger.exception("Failed creating CSV file %s: %s", path, e)
