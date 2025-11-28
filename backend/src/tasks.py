# tasks.py
def simple_task(x, y):
    """Simple test function (importable by RQ workers)."""
    print(f"[TASK] Running: {x} + {y}")
    return x + y
