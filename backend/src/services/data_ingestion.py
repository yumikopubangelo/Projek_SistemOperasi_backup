"""
Data Ingestion Service with safe backpressure & fallback buffer.

Improvements:
 - Accepts legacy `alert_service` kwarg (backwards-compatible)
 - Thread-safe local NDJSON buffer (file lock)
 - Atomic writes when truncating/rewriting buffer file
 - Streaming drain_local_buffer (doesn't load whole file into memory)
 - Clearer logging and defensive checks
"""

import os
import json
import logging
import time
import tempfile
import threading
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# default config; tune as needed
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_RETRIES = 5
DEFAULT_INITIAL_BACKOFF = 0.5  # seconds
DEFAULT_MAX_BACKOFF = 30.0  # seconds
LOCAL_BUFFER_DIR = os.environ.get("AQUAGUARD_LOCAL_BUFFER_DIR", "/tmp/aquaguard_local_buffer")
LOCAL_BUFFER_FILE = os.path.join(LOCAL_BUFFER_DIR, "ingest_buffer.ndjson")


class DataIngestionService:
    def __init__(self,
                 queue_manager=None,
                 csv_exporter=None,
                 es_client=None,
                 alert_service=None,
                 **kwargs):
        """
        queue_manager: AquaGuardQueueManager instance (optional, recommended)
        csv_exporter: optional CSV logger
        es_client: optional ES client for direct indexing fallback
        alert_service: optional (kept for backwards compatibility)
        **kwargs: accept extra args to stay backwards compatible
        """
        self.queue_manager = queue_manager
        self.csv_exporter = csv_exporter
        self.es_client = es_client
        # keep alert_service for backward compatibility even if unused
        self.alert_service = alert_service or kwargs.get("alert_service")

        # ensure buffer directory exists
        try:
            os.makedirs(LOCAL_BUFFER_DIR, exist_ok=True)
        except Exception:
            logger.exception("[DATA_INGEST] Could not create local buffer dir %s", LOCAL_BUFFER_DIR)

        # file lock for local buffer operations (threads in same process)
        self._file_lock = threading.Lock()

    # ---------------- single record ingestion ----------------
    def ingest(self, data: Dict[str, Any]) -> bool:
        """
        Ingest a single record via queue if available, else attempt direct index or fallback to buffer.
        Returns True on successful handoff (queued / indexed / buffered), False otherwise.
        """
        if not isinstance(data, dict):
            logger.warning("[DATA_INGEST] ingest expected dict, got %s", type(data))
            return False

        # Try queue-based ingestion if queue_manager provided
        if self.queue_manager:
            try:
                tid = self.queue_manager.submit_data_ingestion(data, priority=self.queue_manager.TaskPriority.HIGH)
                if tid:
                    logger.debug("[DATA_INGEST] Single record queued: %s", tid)
                    return True
                # queue full -> fallthrough to retry/direct fallback
                logger.warning("[DATA_INGEST] Queue returned None when ingesting single record - will fallback")
            except Exception:
                logger.exception("[DATA_INGEST] submit_data_ingestion failed (will fallback)")

        # If ES client available, attempt best-effort direct index (may block)
        if self.es_client:
            try:
                if hasattr(self.es_client, "index"):
                    self.es_client.index(body=data)
                    logger.info("[DATA_INGEST] Direct-indexed single record as fallback")
                    return True
            except Exception:
                logger.exception("[DATA_INGEST] Direct index failed")

        # Fallback: append to local buffer for later replay
        try:
            self._append_to_local_buffer([data])
            logger.info("[DATA_INGEST] Single record appended to local buffer")
            return True
        except Exception:
            logger.exception("[DATA_INGEST] Failed to append single record to local buffer")
            return False

    # ---------------- batch ingestion with backoff ----------------
    def ingest_batch(
        self,
        batch: List[Dict[str, Any]],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_retries: int = DEFAULT_MAX_RETRIES,
        initial_backoff: float = DEFAULT_INITIAL_BACKOFF,
        max_backoff: float = DEFAULT_MAX_BACKOFF,
        non_blocking: bool = False
    ) -> Dict[str, Any]:
        """
        Ingest a batch of records robustly.
        Returns summary dict with counts and list of buffered filenames if any.
        """
        if not isinstance(batch, list) or not batch:
            return {"status": "error", "message": "batch must be non-empty list"}

        if not isinstance(chunk_size, int) or chunk_size <= 0:
            chunk_size = DEFAULT_CHUNK_SIZE

        total = len(batch)
        success_chunks = 0
        buffered_chunks = 0
        failed_chunks = 0
        buffered_files = []

        # chunking
        for i in range(0, total, chunk_size):
            chunk = batch[i:i + chunk_size]
            submitted = False

            # Decide which queue method to call (prefer submit_batch_ingestion)
            submit_fn = None
            if self.queue_manager and hasattr(self.queue_manager, "submit_batch_ingestion"):
                submit_fn = lambda c: self.queue_manager.submit_batch_ingestion(c, priority=self.queue_manager.TaskPriority.HIGH)
            elif self.queue_manager and hasattr(self.queue_manager, "submit_bulk_data_processing"):
                submit_fn = lambda c: self.queue_manager.submit_bulk_data_processing(c, priority=self.queue_manager.TaskPriority.HIGH)
            else:
                submit_fn = None

            # If no queue manager configured, try direct ES indexing attempt (best-effort)
            if submit_fn is None:
                try:
                    if self.es_client and hasattr(self.es_client, "bulk_index"):
                        success, failed = self.es_client.bulk_index(chunk)
                        if success == len(chunk):
                            success_chunks += 1
                            submitted = True
                        else:
                            logger.warning("[DATA_INGEST] Direct bulk index partial success: %d/%d", success, len(chunk))
                    else:
                        logger.warning("[DATA_INGEST] No queue_manager nor bulk_index available -> buffering chunk")
                except Exception:
                    logger.exception("[DATA_INGEST] Direct bulk_index failed; will buffer chunk")
            else:
                # Attempt submit with retries & backoff
                attempt = 0
                backoff = initial_backoff
                while attempt <= max_retries:
                    try:
                        if non_blocking and attempt > 0:
                            # in non-blocking mode, avoid long waits: break to buffering
                            logger.debug("[DATA_INGEST] non_blocking and retry needed -> will buffer chunk")
                            break

                        tid = submit_fn(chunk)
                        if tid:
                            logger.info("[DATA_INGEST] Chunk submitted to queue (task_id=%s) size=%d", tid, len(chunk))
                            success_chunks += 1
                            submitted = True
                            break
                        else:
                            # queue returned None -> backpressure
                            attempt += 1
                            if attempt > max_retries:
                                logger.warning("[DATA_INGEST] Chunk submission exhausted retries (max=%d)", max_retries)
                                break
                            sleep_time = min(backoff, max_backoff)
                            logger.warning("[DATA_INGEST] Queue full, retrying in %.1fs (attempt %d/%d)", sleep_time, attempt, max_retries)
                            time.sleep(sleep_time)
                            backoff = min(backoff * 2, max_backoff)
                            continue
                    except Exception:
                        attempt += 1
                        logger.exception("[DATA_INGEST] submit attempt raised exception (attempt %d)", attempt)
                        if attempt > max_retries:
                            break
                        time.sleep(min(backoff, max_backoff))
                        backoff = min(backoff * 2, max_backoff)

            if not submitted:
                # persist chunk to local buffer
                try:
                    filename = self._append_to_local_buffer(chunk)
                    buffered_files.append(filename)
                    buffered_chunks += 1
                    logger.info("[DATA_INGEST] Chunk buffered to local file: %s (size=%d)", filename, len(chunk))
                except Exception:
                    logger.exception("[DATA_INGEST] Failed to buffer chunk to local file")
                    failed_chunks += 1

        return {
            "status": "ok",
            "total_records": total,
            "chunk_size": chunk_size,
            "success_chunks": success_chunks,
            "buffered_chunks": buffered_chunks,
            "failed_chunks": failed_chunks,
            "buffered_files": buffered_files
        }

    # ---------------- local buffer helpers ----------------
    def _append_to_local_buffer(self, chunk: List[Dict[str, Any]]) -> str:
        """
        Append a chunk (list of dicts) to NDJSON file. Returns filename (path).
        Uses a simple lock to be thread-safe within the process.
        """
        fname = LOCAL_BUFFER_FILE
        try:
            with self._file_lock:
                # ensure directory exists (again, in case env changed)
                os.makedirs(os.path.dirname(fname), exist_ok=True)
                with open(fname, "a", encoding="utf-8") as fh:
                    for rec in chunk:
                        try:
                            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        except (TypeError, ValueError):
                            # fallback: try to stringify problematic objects
                            fh.write(json.dumps({"_bad_record": True, "repr": repr(rec)}, ensure_ascii=False) + "\n")
            return fname
        except Exception:
            logger.exception("[DATA_INGEST] Failed to append to local buffer file %s", fname)
            raise

    def drain_local_buffer(self, max_chunks: Optional[int] = None, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Dict[str, Any]:
        """
        Attempt to replay buffered records into the queue in FIFO order.

        Implementation notes:
         - Streams the buffer file line-by-line to avoid loading the whole file.
         - Writes remaining lines atomically to a temp file and replaces original.
        """
        if not os.path.exists(LOCAL_BUFFER_FILE):
            return {"status": "ok", "message": "no buffer file"}

        processed_chunks = 0
        success_records = 0
        failed_records = 0
        remaining_lines = []

        # We'll stream lines and attempt to submit chunk by chunk.
        current_chunk = []
        lines_consumed = 0

        try:
            with self._file_lock:
                # Open original file for reading
                with open(LOCAL_BUFFER_FILE, "r", encoding="utf-8") as fh:
                    for raw_line in fh:
                        line = raw_line.rstrip("\n")
                        if not line:
                            continue
                        lines_consumed += 1
                        try:
                            rec = json.loads(line)
                        except Exception:
                            # skip bad JSON lines but log them
                            logger.warning("[DATA_INGEST] Skipping invalid JSON line in buffer (line %d)", lines_consumed)
                            failed_records += 1
                            continue

                        current_chunk.append(rec)

                        if len(current_chunk) >= chunk_size:
                            # attempt to submit this chunk
                            res = self.ingest_batch(current_chunk, chunk_size=chunk_size, non_blocking=False)
                            processed_chunks += 1
                            if res.get("success_chunks", 0) > 0:
                                success_records += len(current_chunk)
                            else:
                                # couldn't submit: keep these records (prepend to remaining_lines)
                                for r in current_chunk:
                                    remaining_lines.append(r)
                            current_chunk = []

                            if max_chunks is not None and processed_chunks >= max_chunks:
                                # stop processing further; append rest of lines to remaining
                                # Read rest of file and add to remaining_lines
                                for rest_line in fh:
                                    rest_line = rest_line.rstrip("\n")
                                    if not rest_line:
                                        continue
                                    try:
                                        rest_rec = json.loads(rest_line)
                                        remaining_lines.append(rest_rec)
                                    except Exception:
                                        failed_records += 1
                                break

                # after file read, if some items remain in current_chunk, process them
                if current_chunk:
                    res = self.ingest_batch(current_chunk, chunk_size=chunk_size, non_blocking=False)
                    processed_chunks += 1
                    if res.get("success_chunks", 0) > 0:
                        success_records += len(current_chunk)
                    else:
                        for r in current_chunk:
                            remaining_lines.append(r)

                # Now rewrite the buffer file atomically with remaining_lines
                # If no remaining, remove file
                if remaining_lines:
                    tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(LOCAL_BUFFER_FILE))
                    try:
                        with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmp_fh:
                            for rec in remaining_lines:
                                try:
                                    tmp_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                                except Exception:
                                    tmp_fh.write(json.dumps({"_bad_record": True, "repr": repr(rec)}, ensure_ascii=False) + "\n")
                        # atomic replace
                        os.replace(tmp_path, LOCAL_BUFFER_FILE)
                        logger.info("[DATA_INGEST] Drain completed; remaining records: %d", len(remaining_lines))
                    except Exception:
                        logger.exception("[DATA_INGEST] Failed to write remaining buffer file atomically")
                        # cleanup tmp if exists
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass
                        return {"status": "error", "message": "rewrite_failed"}
                else:
                    # no remaining lines -> remove file
                    try:
                        os.remove(LOCAL_BUFFER_FILE)
                    except FileNotFoundError:
                        pass
                    except Exception:
                        # if remove fails, truncate file
                        try:
                            with open(LOCAL_BUFFER_FILE, "w", encoding="utf-8") as fh:
                                pass
                        except Exception:
                            logger.exception("[DATA_INGEST] Failed to truncate buffer file after drain")
                    logger.info("[DATA_INGEST] Drain completed; buffer emptied")

        except Exception:
            logger.exception("[DATA_INGEST] Unexpected error during drain_local_buffer")
            return {"status": "error", "message": "drain_failed"}

        return {
            "status": "ok",
            "processed_chunks": processed_chunks,
            "success_records": success_records,
            "failed_records": failed_records,
            "remaining_records": len(remaining_lines)
        }
