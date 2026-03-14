import os
from redis import Redis
from rq import Worker, Queue, Connection

listen = ["default"]
redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
conn = Redis.from_url(redis_url)

if __name__ == "__main__":
    with Connection(conn):
        worker = Worker([Queue(name) for name in listen])
        worker.work()
