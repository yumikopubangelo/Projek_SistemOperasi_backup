from redis import Redis
from rq import Queue
from tasks import simple_task   # <- import dari modul, bukan __main__

redis_conn = Redis.from_url("redis://localhost:6379/0")
q = Queue("aquaguard", connection=redis_conn)

print("[TEST] Submitting test task...")
job = q.enqueue(simple_task, args=(5, 10), timeout=10)
print(f"✅ Job submitted: {job.id}")

# Wait for result...
import time
for i in range(10):
    job.refresh()
    if job.is_finished:
        print(f"✅ Job completed! Result: {job.result}")
        break
    elif job.is_failed:
        print(f"❌ Job failed! Error: {job.exc_info}")
        break
    print(f"   Status: {job.get_status()}")
    time.sleep(1)
else:
    print("⏱️ Timeout waiting for job")
