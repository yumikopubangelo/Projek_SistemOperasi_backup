"""
Debug Failed RQ Jobs
"""

from redis import Redis
from rq import Queue
from rq.registry import FailedJobRegistry

redis_conn = Redis.from_url("redis://localhost:6379/0")
queue = Queue("aquaguard", connection=redis_conn)

# Get failed jobs registry
failed_registry = FailedJobRegistry(queue=queue)

print(f"Total failed jobs: {len(failed_registry)}")
print("=" * 60)

for job_id in failed_registry.get_job_ids():
    job = queue.fetch_job(job_id)
    print(f"\nJob ID: {job_id}")
    print(f"Function: {job.func_name}")
    print(f"Args: {job.args}")
    print(f"Created: {job.created_at}")
    print(f"Failed: {job.ended_at}")
    print(f"\nError:")
    print(job.exc_info)  # Full traceback
    print("=" * 60)