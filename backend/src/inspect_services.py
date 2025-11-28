# inspect_services.py
from importlib import import_module
import traceback

try:
    app_mod = import_module("app")
except Exception:
    print("FAILED import app — check path and run from project root")
    traceback.print_exc()
    raise SystemExit(1)

# try both create_app and initialize_services
services = None
if hasattr(app_mod, "initialize_services"):
    try:
        services = app_mod.initialize_services()
        print("initialize_services() returned type:", type(services))
    except Exception as e:
        print("initialize_services() raised:", repr(e))
        traceback.print_exc()
else:
    print("app.initialize_services not found")

# if create_app exists, try that too
if hasattr(app_mod, "create_app"):
    try:
        app = app_mod.create_app()
        print("create_app() returned Flask app:", getattr(app, "name", None))
        s = getattr(app, "config", {}).get("SERVICES") or getattr(app, "services", None)
        print("services in app (from create_app):", bool(s))
        if s:
            print("services keys:", sorted(list(s.keys())))
            print("queue_manager present?:", "queue_manager" in s and bool(s.get("queue_manager")))
    except Exception as e:
        print("create_app() raised:", repr(e))
        traceback.print_exc()
