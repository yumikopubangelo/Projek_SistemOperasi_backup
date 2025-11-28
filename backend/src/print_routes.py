# simpan sebagai print_routes.py lalu jalankan: python print_routes.py
from importlib import import_module
import sys
import inspect

# Sesuaikan import_path/app_attr jika app Anda bukan di 'app.py' atau variabelnya bukan 'app'
import_path = "app"   # ganti 'app' jika Flask app Anda diekspor dari module lain (mis. "src.app")
app_attr = "app"

try:
    mod = import_module(import_path)
    app = getattr(mod, app_attr)
except Exception as e:
    print("ERROR loading app:", e)
    sys.exit(1)

rules = sorted(app.url_map.iter_rules(), key=lambda r: (r.rule, r.endpoint))
for r in rules:
    print(f"Rule: {r.rule}  endpoint: {r.endpoint}  methods: {set(r.methods)}")
    view = app.view_functions.get(r.endpoint)
    if view:
        try:
            print("  -> view func:", view.__name__, "defined in", inspect.getsourcefile(view), "line", inspect.getsourcelines(view)[1])
        except Exception as e:
            print("  -> view func info failed:", e)
    print()
