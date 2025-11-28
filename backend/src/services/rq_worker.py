"""
RQ worker bootstrapper for AquaGuard with robust config loading and Windows compatibility.

Features preserved:
 - Loads environment variables from DOTENV_PATH, project_root/.env, project_root/core/.env, or find_dotenv().
 - Prompts (secure) for missing sensitive variables when interactive.
 - Calls app.initialize_services() to construct SERVICES and injects it into services.rq_tasks.
 - Starts an RQ worker; uses SimpleWorker on platforms without os.fork (Windows), otherwise standard Worker.
"""

import logging
import os
import sys
import getpass
import platform
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Required keys that Config.validate() needs (adjust if your Config expects different)
REQUIRED_ENV_KEYS = [
    "ELASTIC_HOST",
    "ELASTIC_USER",
    "ELASTIC_PASS",
    "ELASTIC_INDEX",
    "SECRET_KEY",
]

REDIS_URL_ENV = "REDIS_URL"
RQ_QUEUE_NAME_ENV = "RQ_QUEUE_NAME"

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_RQ_NAME = "aquaguard"


# ------------------------------------------------------------
# Helpers: dotenv loading & interactive prompt
# ------------------------------------------------------------
def missing_env_keys(keys: List[str]) -> List[str]:
    return [k for k in keys if not os.environ.get(k)]


def try_load_dotenv(project_root: str) -> None:
    """Load .env from multiple common locations (DOTENV_PATH, root/.env, core/.env, find_dotenv)."""
    try:
        from dotenv import load_dotenv, find_dotenv
    except Exception:
        logger.debug("[WORKER] python-dotenv not installed, skipping .env loading")
        return

    # 1) DOTENV_PATH override
    override = os.environ.get("DOTENV_PATH")
    if override and os.path.exists(override):
        logger.info("[WORKER] Loading environment from DOTENV_PATH=%s", override)
        load_dotenv(override, override=False)
        return

    # 2) project_root/.env
    root_env = os.path.join(project_root, ".env")
    if os.path.exists(root_env):
        logger.info("[WORKER] Loading environment from %s", root_env)
        load_dotenv(root_env, override=False)
        return

    # 3) project_root/core/.env
    core_env = os.path.join(project_root, "core", ".env")
    if os.path.exists(core_env):
        logger.info("[WORKER] Loading environment from %s", core_env)
        load_dotenv(core_env, override=False)
        return

    # 4) fallback find_dotenv()
    found = find_dotenv(raise_error_if_not_found=False)
    if found:
        logger.info("[WORKER] Loading environment from discovered .env: %s", found)
        load_dotenv(found, override=False)
        return

    logger.debug("[WORKER] No .env found in standard locations")


def prompt_for_missing_keys(keys: List[str]) -> None:
    """Prompt user securely for environment variables (interactive shells only)."""
    if not keys:
        return
    if not sys.stdin.isatty():
        raise RuntimeError(f"Missing env vars {keys} and no TTY available to prompt.")

    logger.info("[WORKER] Prompting for %d missing environment variables...", len(keys))

    for k in keys:
        if "PASS" in k or "SECRET" in k or "KEY" in k:
            val = getpass.getpass(f"Enter value for {k}: ")
        else:
            try:
                val = input(f"Enter value for {k}: ")
            except Exception:
                val = ""
        if val:
            os.environ[k] = val
            logger.info("[WORKER] Set env %s (hidden)", k)
        else:
            logger.warning("[WORKER] No value entered for %s (may cause failure)", k)


def ensure_config_ready(project_root: str) -> None:
    """
    Ensure Config.validate() will succeed by guaranteeing required env vars available.
    """
    missing = missing_env_keys(REQUIRED_ENV_KEYS)
    if not missing:
        logger.info("[WORKER] ✅ All required environment variables already set")
        return

    # Try loading .env files
    try_load_dotenv(project_root)
    missing = missing_env_keys(REQUIRED_ENV_KEYS)
    if not missing:
        logger.info("[WORKER] ✅ Required keys loaded from .env or environment")
        return

    # Prompt if running interactively
    if sys.stdin.isatty():
        prompt_for_missing_keys(missing)
        missing = missing_env_keys(REQUIRED_ENV_KEYS)
        if not missing:
            logger.info("[WORKER] ✅ Required env keys provided via prompt")
            return
        else:
            raise RuntimeError(f"Still missing env vars after prompt: {missing}")

    # Non-interactive and still missing: fail fast
    raise RuntimeError(f"Missing required env vars: {missing}. Worker cannot proceed.")


# ------------------------------------------------------------
# Main entrypoint
# ------------------------------------------------------------
def main():
    project_root = os.getcwd()
    logger.info("[WORKER] Project root: %s", project_root)

    # ensure project root on sys.path
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Load environment BEFORE importing app
    try:
        # Provide a debug message about where we attempt to load .env from
        try_load_dotenv(project_root)
        # If dotenv loaded something, log it
        env_source = os.environ.get("DOTENV_PATH") or os.path.join(project_root, ".env")
        logger.info("[CONFIG] Loading .env from: %s", env_source)
    except Exception:
        # non-fatal here; ensure_config_ready will try again/prompt
        logger.debug("[WORKER] try_load_dotenv returned with no effect or missing python-dotenv")

    # Ensure config ready (may prompt)
    try:
        ensure_config_ready(project_root)
    except Exception as e:
        logger.exception("[WORKER] Configuration not ready: %s", e)
        sys.exit(1)

    # Import app and initialize services
    logger.info("[WORKER] Importing app module...")
    try:
        import app as app_module  # expects app.py in project root
    except Exception as e:
        logger.exception("[WORKER] Failed to import app module: %s", e)
        sys.exit(1)

    logger.info("[WORKER] Calling app.initialize_services()")
    try:
        SERVICES = app_module.initialize_services()
    except Exception as e:
        logger.exception("[WORKER] initialize_services() failed: %s", e)
        sys.exit(1)

    # Inject SERVICES into task module(s)
    try:
        import services.rq_tasks as rq_tasks
        rq_tasks.SERVICES = SERVICES
        logger.info("[WORKER] ✅ SERVICES injected into services.rq_tasks")
    except Exception as e:
        logger.exception("[WORKER] Failed to inject SERVICES into services.rq_tasks: %s", e)
        sys.exit(1)

    # Setup Redis + RQ
    try:
        from redis import Redis
        from rq import Queue
    except Exception as e:
        logger.exception("[WORKER] redis/rq libraries missing: %s", e)
        sys.exit(1)

    redis_url = os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL)
    queue_name = os.environ.get(RQ_QUEUE_NAME_ENV, DEFAULT_RQ_NAME)

    logger.info("[WORKER] Connecting to Redis at %s (queue=%s)", redis_url, queue_name)
    try:
        redis_conn = Redis.from_url(redis_url)
        # quick ping
        redis_conn.ping()
        logger.info("[WORKER] ✅ Redis connection successful")
    except Exception as e:
        logger.exception("[WORKER] Could not connect to Redis: %s", e)
        sys.exit(1)

    q = Queue(queue_name, connection=redis_conn)

    # Choose worker class: prefer forking Worker when os.fork is available; otherwise use SimpleWorker (Windows)
    use_simple_worker = False
    try:
        has_fork = hasattr(os, "fork") and callable(getattr(os, "fork"))
        if not has_fork:
            use_simple_worker = True
    except Exception:
        use_simple_worker = True

    # Import worker classes
    try:
        # SimpleWorker exists in rq.worker
        from rq.worker import SimpleWorker
    except Exception:
        SimpleWorker = None  # type: ignore

    # Default to rq.Worker if fork available and SimpleWorker not explicitly required
    worker_cls = None
    if use_simple_worker or platform.system() == "Windows" or SimpleWorker is None:
        # prefer SimpleWorker on Windows or where fork isn't available
        if SimpleWorker is None:
            logger.warning("[WORKER] SimpleWorker not available in this rq version; attempting to use Worker (may fail on Windows)")
            from rq import Worker as FallbackWorker
            worker_cls = FallbackWorker
            logger.info("[WORKER] Using rq.Worker as fallback")
        else:
            worker_cls = SimpleWorker
            logger.info("[WORKER] Windows/No-fork environment detected -> using rq.worker.SimpleWorker (no fork)")
    else:
        from rq import Worker as ForkingWorker
        worker_cls = ForkingWorker
        logger.info("[WORKER] Fork available -> using rq.Worker (forking workhorse)")

    # Instantiate and run worker
    try:
        worker = worker_cls([q], connection=redis_conn)
        logger.info("======================================================================")
        logger.info("WORKER READY - Listening on queue '%s'", queue_name)
        logger.info("======================================================================")
        # This call blocks and runs the worker loop
        worker.work()
    except Exception as e:
        logger.exception("[WORKER] Worker main loop failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
