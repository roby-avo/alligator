import os
import redis

REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])

job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)
stop = job_active.exists("STOP")
print(stop)
