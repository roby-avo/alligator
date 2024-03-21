import os
import redis
import time

REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])

# Function to get the 'STOP' status from Redis with retry logic
def get_stop_status_from_redis(retries=5, delay=3):
    try:
        job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)
        return job_active.exists("STOP")
    except redis.exceptions.ConnectionError as e:
        if retries > 0:
            print(f"ConnectionError: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            return get_stop_status_from_redis(retries - 1, delay)
        else:
            raise Exception("Max retries reached. Redis server is not responding.")

# Call the function to get the 'STOP' status
stop = get_stop_status_from_redis()
print(stop)
