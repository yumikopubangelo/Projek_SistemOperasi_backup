"""
Enhanced Queue Service - WITH EXTENSIVE DEBUGGING
"""

import logging
import time
import threading
import uuid
import os
from datetime import datetime
from typing import Dict, Any, Optional, Callable, Tuple
from enum import Enum
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Try to import Redis & RQ
try:
    from redis import Redis
    from rq import Queue
    from rq.job import Job
    _HAS_RQ = True
    logger.info("âœ… RQ libraries imported successfully")
except Exception as e:
    Redis = None
    Queue = None
    Job = None
    _HAS_RQ = False
    logger.warning(f"âš ï¸ RQ import failed: {e}")


# ============= enums / dataclasses =============
class TaskPriority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class Task:
    id: str
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.MEDIUM
    timeout: float = 300.0
    max_retries: int = 3
    retries: int = 0

    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    result: Any = None

    paused: bool = False
    cancelled: bool = False


# ---------------- default Redis config ----------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
RQ_QUEUE_NAME = os.environ.get("RQ_QUEUE_NAME", "aquaguard")
RQ_EXECUTOR_FN = os.environ.get("RQ_EXECUTOR_FN", "services.rq_tasks.execute_callable")


# ============= EnhancedQueueService (RQ-capable) =============
class EnhancedQueueService:
    def __init__(self,
                 max_workers: int = 5,
                 max_queue_size: int = 100,
                 redis_url: str = REDIS_URL,
                 queue_name: str = RQ_QUEUE_NAME,
                 health_check_interval: Optional[int] = None,
                 circuit_breaker_threshold: Optional[int] = None,
                 circuit_breaker_timeout: Optional[int] = None,
                 use_rq: bool = True):  # NEW: explicit control
        
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size

        # internal queues by priority (used only in inproc mode)
        self.high = deque()
        self.medium = deque()
        self.low = deque()

        # bookkeeping (inproc mode)
        self.active_tasks: Dict[str, Task] = {}
        self.paused_tasks: Dict[str, Task] = {}
        self.dead_letter: Dict[str, Task] = {}

        self.total_received = 0
        self.total_completed = 0
        self.total_failed = 0
        self.total_timeout = 0

        # locks (inproc)
        self.queue_lock = threading.Lock()
        self.task_lock = threading.Lock()

        # control (inproc)
        self._running = True
        self._workers = []

        # RQ/Redis state
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.rq_enabled = False
        self.redis = None
        self.rq = None

        logger.info("=" * 80)
        logger.info("QUEUE SERVICE INITIALIZATION - DEBUG MODE")
        logger.info("=" * 80)
        logger.info(f"RQ libraries available: {_HAS_RQ}")
        logger.info(f"DISABLE_RQ env var: {os.environ.get('DISABLE_RQ', 'not set')}")
        logger.info(f"use_rq parameter: {use_rq}")
        logger.info(f"Redis URL: {redis_url}")
        logger.info(f"Queue name: {queue_name}")

        # attempt connect to Redis + RQ if available
        if _HAS_RQ and os.environ.get("DISABLE_RQ", "").lower() != "true" and use_rq:
            try:
                logger.info(f"[RQ] Attempting to connect to Redis at {redis_url}...")
                self.redis = Redis.from_url(self.redis_url, socket_connect_timeout=5)
                
                # Test connectivity with timeout
                logger.info("[RQ] Testing Redis connection with PING...")
                ping_result = self.redis.ping()
                logger.info(f"[RQ] Redis PING result: {ping_result}")
                
                # Create RQ queue
                logger.info(f"[RQ] Creating RQ Queue object for '{queue_name}'...")
                self.rq = Queue(self.queue_name, connection=self.redis)
                
                # Test queue operations
                logger.info("[RQ] Testing queue operations...")
                queue_len = self.redis.llen(self.rq.name)
                logger.info(f"[RQ] Current queue length: {queue_len}")
                
                self.rq_enabled = True
                logger.info("âœ… [RQ] Redis/RQ initialized successfully!")
                logger.info(f"[RQ] Queue key in Redis: {self.rq.name}")
                
            except Exception as e:
                logger.error(f"âŒ [RQ] Redis/RQ initialization failed: {type(e).__name__}: {e}")
                logger.exception("[RQ] Full traceback:")
                self.redis = None
                self.rq = None
                self.rq_enabled = False
                logger.warning("[RQ] Falling back to in-process queue")
        else:
            if os.environ.get("DISABLE_RQ") == "true":
                logger.info("[QUEUE] RQ disabled via DISABLE_RQ env var")
            elif not use_rq:
                logger.info("[QUEUE] RQ disabled via use_rq=False parameter")
            elif not _HAS_RQ:
                logger.warning("[QUEUE] RQ libraries not available")
            else:
                logger.info("[QUEUE] Using in-process queue (RQ not requested)")

        # Expose TaskPriority enum
        self.TaskPriority = TaskPriority

        logger.info("=" * 80)
        logger.info(f"FINAL STATUS: rq_enabled={self.rq_enabled}, mode={'RQ' if self.rq_enabled else 'IN-PROCESS'}")
        logger.info("=" * 80)

        # Start in-process workers if not using RQ
        if not self.rq_enabled:
            self._start_workers()

    # ---------------- worker lifecycle (inproc) ----------------
    def _start_workers(self):
        logger.info("[INPROC] Starting in-process workers...")
        for i in range(max(1, self.max_workers)):
            t = threading.Thread(target=self._worker_loop, name=f"queue-worker-{i}", daemon=True)
            t.start()
            self._workers.append(t)
        logger.info(f"[INPROC] Started {len(self._workers)} in-process workers")

    def _stop_workers(self):
        self._running = False
        with self.queue_lock:
            pass
        for w in self._workers:
            try:
                if w.is_alive():
                    w.join(timeout=1.0)
            except Exception:
                pass
        logger.info("[INPROC] In-process workers stopped")

    # ---------------- submitting (unified) ----------------
    def submit_task(self,
                    name: str,
                    func: Callable,
                    args: tuple = (),
                    kwargs: Optional[dict] = None,
                    priority: TaskPriority = TaskPriority.MEDIUM,
                    timeout: float = 300.0,
                    max_retries: int = 3) -> Optional[str]:
        """
        Submit a task. In RQ-mode we enqueue a job; in inproc-mode we push to local deque.
        Returns a task_id (RQ job id or inproc UUID) or None if rejected.
        """
        if kwargs is None:
            kwargs = {}

        logger.info("=" * 80)
        logger.info(f"SUBMIT_TASK CALLED: name={name}, priority={priority.name}")
        logger.info(f"RQ enabled: {self.rq_enabled}")
        logger.info(f"RQ object: {self.rq is not None}")
        logger.info("=" * 80)

        # If RQ enabled, try to enqueue to RQ
        if self.rq_enabled and self.rq is not None:
            try:
                func_path = self._serialize_callable(func)
                logger.info(f"[RQ] Serialized callable: {func_path}")
                
                fqfn = RQ_EXECUTOR_FN
                logger.info(f"[RQ] Executor function: {fqfn}")
                logger.info(f"[RQ] Attempting to enqueue to queue '{self.queue_name}'...")
                
                # CRITICAL: Add extensive error handling
                job = self.rq.enqueue_call(
                    func=fqfn,
                    args=(func_path, args, kwargs),
                    timeout=int(timeout),
                    result_ttl=3600,
                    failure_ttl=3600  # Keep failed jobs for debugging
                )
                
                job_id = job.get_id()
                logger.info(f"âœ… [RQ] Job enqueued successfully!")
                logger.info(f"[RQ] Job ID: {job_id}")
                logger.info(f"[RQ] Job status: {job.get_status()}")
                
                # Verify job in queue
                queue_len = self.redis.llen(self.rq.name)
                logger.info(f"[RQ] Current queue length: {queue_len}")
                
                # Store metadata
                try:
                    meta_key = f"aquaguard:jobmeta:{job_id}"
                    meta = {
                        "name": name,
                        "submitted_at": datetime.utcnow().isoformat() + "Z",
                        "timeout": timeout,
                        "max_retries": max_retries,
                        "priority": priority.name,
                        "func_path": func_path
                    }
                    self.redis.hset(meta_key, mapping=meta)
                    self.redis.expire(meta_key, 3600)
                    logger.info(f"[RQ] Metadata stored: {meta_key}")
                except Exception as meta_err:
                    logger.warning(f"[RQ] Failed to store metadata (non-fatal): {meta_err}")

                logger.info("=" * 80)
                return job_id
                
            except Exception as e:
                logger.error("âŒ [RQ] ENQUEUE FAILED!")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(f"Error message: {e}")
                logger.exception("Full traceback:")
                logger.warning("[RQ] Falling back to in-process queue for this task")
                # Don't return None here - fallthrough to inproc
        else:
            logger.info("[QUEUE] RQ not enabled, using in-process queue")

        # Fall back to in-process submit
        logger.info(f"[INPROC] Submitting to in-process queue: {name}")
        result = self._submit_task_inproc(name, func, args, kwargs, priority, timeout, max_retries)
        logger.info(f"[INPROC] Submit result: {result}")
        logger.info("=" * 80)
        return result

    # --------- in-process submit (unchanged behavior) ----------
    def _submit_task_inproc(self, name: str, func: Callable, args: tuple, kwargs: dict,
                            priority: TaskPriority, timeout: float, max_retries: int) -> Optional[str]:
        with self.queue_lock:
            if self._queue_len() >= self.max_queue_size:
                logger.warning("[QUEUE] Rejecting task submit (%s) - queue full (%d >= %d)",
                               name, self._queue_len(), self.max_queue_size)
                return None

            tid = str(uuid.uuid4())
            task = Task(
                id=tid,
                name=name,
                func=func,
                args=args,
                kwargs=kwargs,
                priority=priority,
                timeout=timeout,
                max_retries=max_retries
            )

            if priority == TaskPriority.HIGH:
                self.high.append(task)
            elif priority == TaskPriority.LOW:
                self.low.append(task)
            else:
                self.medium.append(task)

            self.total_received += 1
            logger.debug("[QUEUE] (inproc) Submitted %s (%s) priority=%s", tid, name, priority.name)
            return tid

    def _queue_len(self):
        return len(self.high) + len(self.medium) + len(self.low)

    # ... (rest of the methods remain the same)
    # Copy from original queue_service.py: _pop_next, _worker_loop, _execute_task, etc.

    def _serialize_callable(self, func: Callable) -> str:
        """Convert callable to module path string"""
        if isinstance(func, str):
            return func
        try:
            mod = getattr(func, "__module__", None)
            name = getattr(func, "__name__", None)
            if mod and name:
                return f"{mod}.{name}"
        except Exception:
            pass
        return repr(func)

    def get_stats(self) -> Dict[str, Any]:
        try:
            if self.rq_enabled and self.redis and self.rq:
                try:
                    # best-effort queue length
                    try:
                        qlen = int(self.redis.llen(self.rq.name))
                    except Exception:
                        qlen = None

                    # attempt to count workers safely across RQ versions
                    workers = None
                    try:
                        # some wrappers expose .workers
                        if hasattr(self.rq, "workers"):
                            try:
                                workers = len([w for w in self.rq.workers if not getattr(w, "stopped", False)])
                            except Exception:
                                try:
                                    workers = len(self.rq.workers)
                                except Exception:
                                    workers = None
                        else:
                            # fallback to rq.Worker.all()
                            try:
                                from rq import Worker as RQWorker
                                try:
                                    workers = len(list(RQWorker.all(connection=self.redis)))
                                except Exception:
                                    workers = len(list(RQWorker.all()))
                            except Exception:
                                workers = None
                    except Exception:
                        workers = None

                    return {
                        "mode": "rq",
                        "queue": getattr(self.rq, "name", self.queue_name),
                        "queued": qlen,
                        "connected": True,
                        "workers": workers,
                        "redis_url": self.redis_url
                    }
                except Exception as e:
                    logger.exception("[QUEUE][RQ] get_stats failed")
                    return {
                        "mode": "rq",
                        "queue": self.queue_name,
                        "queued": None,
                        "connected": False,
                        "error": str(e)
                    }
            else:
                # in-process stats
                with self.queue_lock, self.task_lock:
                    return {
                        "mode": "inproc",
                        "queued_high": len(self.high),
                        "queued_medium": len(self.medium),
                        "queued_low": len(self.low),
                        "queued_total": self._queue_len(),
                        "active_count": len(self.active_tasks),
                        "paused_count": len(getattr(self, "paused_tasks", {})),
                        "dead_letter_count": len(getattr(self, "dead_letter", {})),
                        "total_received": getattr(self, "total_received", 0),
                        "total_completed": getattr(self, "total_completed", 0),
                        "total_failed": getattr(self, "total_failed", 0),
                        "total_timeout": getattr(self, "total_timeout", 0),
                        "max_workers": getattr(self, "max_workers", 0),
                        "max_queue_size": getattr(self, "max_queue_size", 0)
                    }
        except Exception:
            logger.exception("[QUEUE] get_stats unexpected error")
            return {"mode": "unknown", "error": "stats_failed"}


    def shutdown(self, wait: float = 5.0):
        logger.info("[QUEUE] Shutdown initiated")
        if not self.rq_enabled:
            stop_by = time.time() + wait
            while time.time() < stop_by and (len(self.active_tasks) > 0 or self._queue_len() > 0):
                logger.info("[QUEUE] Waiting for %d active tasks and %d queued tasks",
                            len(self.active_tasks), self._queue_len())
                time.sleep(0.5)
            self._stop_workers()
        logger.info("[QUEUE] Shutdown complete")