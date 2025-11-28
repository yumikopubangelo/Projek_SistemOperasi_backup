"""
Debug RQ Worker Startup Script
Tests Redis connection and queue operations before starting worker
"""

import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def test_redis_connection():
    """Test Redis connection before starting worker"""
    try:
        from redis import Redis
        from rq import Queue
        from rq.worker import Worker

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        queue_name = os.environ.get("RQ_QUEUE_NAME", "aquaguard")

        logger.info("=" * 80)
        logger.info("REDIS CONNECTION TEST")
        logger.info("=" * 80)
        logger.info(f"Redis URL: {redis_url}")
        logger.info(f"Queue name: {queue_name}")

        # Connect to Redis
        logger.info("Connecting to Redis...")
        redis_conn = Redis.from_url(redis_url, socket_connect_timeout=5)

        # Test PING
        logger.info("Testing PING...")
        ping_result = redis_conn.ping()
        logger.info(f"✓ PING result: {ping_result}")

        # Get Redis info
        logger.info("Getting Redis info...")
        info = redis_conn.info()
        logger.info(f"✓ Redis version: {info.get('redis_version')}")
        logger.info(f"✓ Connected clients: {info.get('connected_clients')}")
        logger.info(f"✓ Used memory: {info.get('used_memory_human')}")

        # Create queue
        logger.info(f"Creating RQ Queue '{queue_name}'...")
        queue = Queue(queue_name, connection=redis_conn)
        logger.info(f"✓ Queue created: {queue}")

        # Check queue length (use queue.key_name / name)
        try:
            queue_key = queue.name  # should be 'aquaguard'
            queue_len = redis_conn.llen(f"rq:queue:{queue_key}")
        except Exception:
            # fallback: try using redis directly with queue.name as key
            try:
                queue_len = redis_conn.llen(queue.name)
            except Exception:
                queue_len = 0
        logger.info(f"✓ Current queue length: {queue_len}")

        # List some RQ-related keys (safe decode)
        logger.info("Listing Redis keys (prefix 'rq:*')...")
        raw_keys = redis_conn.keys("rq:*")
        keys = []
        for k in raw_keys:
            try:
                keys.append(k.decode() if isinstance(k, (bytes, bytearray)) else str(k))
            except Exception:
                keys.append(str(k))
        logger.info(f"✓ Found {len(keys)} RQ-related keys")
        for key in keys[:10]:
            logger.info(f"  - {key}")

        # Check workers - use rq.worker.Worker.all(...)
        logger.info("Checking for active workers via rq.worker.Worker.all()...")
        try:
            workers = Worker.all(connection=redis_conn)
            logger.info(f"✓ Active workers: {len(workers)}")
            for w in workers:
                try:
                    # Worker has attributes `name` and `get_state()` depending on rq version
                    w_name = getattr(w, "name", str(w))
                    # For state, prefer `get_state()` if available, else `state` attribute
                    w_state = None
                    if hasattr(w, "get_state"):
                        try:
                            w_state = w.get_state()
                        except Exception:
                            w_state = getattr(w, "state", "unknown")
                    else:
                        w_state = getattr(w, "state", "unknown")
                    logger.info(f"  - {w_name}: {w_state}")
                except Exception as e:
                    logger.debug(f"  - Worker inspection failed: {e}")
        except Exception as e:
            logger.warning(f"Could not list RQ workers via Worker.all(): {e}")
            # Not fatal — continue; worker list may not be available in older rq versions

        logger.info("=" * 80)
        logger.info("✓ REDIS CONNECTION TEST PASSED")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error("=" * 80)
        logger.error("✗ REDIS CONNECTION TEST FAILED")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {e}")
        logger.exception("Full traceback:")
        logger.error("=" * 80)
        return False


def start_worker():
    """Start RQ worker with enhanced logging"""
    try:
        # Load environment
        from dotenv import load_dotenv
        env_file = "core/.env"
        if os.path.exists(env_file):
            logger.info(f"Loading environment from {env_file}")
            load_dotenv(env_file)

        # Import app and initialize services
        logger.info("Importing app module...")
        import app as app_module

        logger.info("Initializing services...")
        SERVICES = app_module.initialize_services()

        # Inject services into rq_tasks
        logger.info("Injecting SERVICES into rq_tasks...")
        import services.rq_tasks as rq_tasks
        rq_tasks.SERVICES = SERVICES
        logger.info("✓ SERVICES injected")

        # Setup Redis + RQ
        from redis import Redis
        from rq import Queue, Worker
        from rq.worker import SimpleWorker
        import platform

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        queue_name = os.environ.get("RQ_QUEUE_NAME", "aquaguard")

        logger.info("=" * 80)
        logger.info("STARTING RQ WORKER")
        logger.info("=" * 80)
        logger.info(f"Redis URL: {redis_url}")
        logger.info(f"Queue name: {queue_name}")
        logger.info(f"Platform: {platform.system()}")

        # Connect
        redis_conn = Redis.from_url(redis_url)
        queue = Queue(queue_name, connection=redis_conn)

        # Choose worker class
        if platform.system() == "Windows" or not hasattr(os, "fork"):
            worker_cls = SimpleWorker
            logger.info("Using SimpleWorker (no fork)")
        else:
            # prefer rq.Worker when fork is available
            try:
                from rq import Worker as ForkingWorker
                worker_cls = ForkingWorker
                logger.info("Using Worker (forking)")
            except Exception:
                worker_cls = SimpleWorker
                logger.info("Falling back to SimpleWorker (fork not available)")

        # Create and start worker
        worker = worker_cls([queue], connection=redis_conn)

        logger.info("=" * 80)
        logger.info(f"✓ WORKER READY - Listening on '{queue_name}'")
        logger.info("=" * 80)

        # Start worker loop (blocking)
        # Use DEBUG logging level for detailed logs
        worker.work(logging_level="DEBUG")

    except Exception as e:
        logger.exception("Worker startup failed")
        sys.exit(1)


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("AQUAGUARD RQ WORKER - DEBUG MODE")
    logger.info("=" * 80)

    # Step 1: Test Redis connection
    if not test_redis_connection():
        logger.error("✗ Redis connection test failed - cannot start worker")
        sys.exit(1)

    logger.info("\n")
    time.sleep(1)

    # Step 2: Start worker
    start_worker()
