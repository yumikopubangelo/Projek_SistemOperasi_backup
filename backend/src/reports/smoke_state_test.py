import requests, traceback, socket, sys
from datetime import datetime

urls = [
    "https://api.aquaguard.sbs/stats",
    "https://api.aquaguard.sbs/queue/stats"
]

print("Python:", sys.version)
print("requests:", requests.__version__)
print("Time:", datetime.utcnow().isoformat(), "Z")
for u in urls:
    try:
        print("\n-> GET", u)
        r = requests.get(u, timeout=5)
        print("  OK", r.status_code, "len", len(r.content))
        print("  X-Response-Time:", r.headers.get("X-Response-Time"))
    except Exception as e:
        print("  EXCEPTION:", type(e).__name__, str(e))
        traceback.print_exc()
