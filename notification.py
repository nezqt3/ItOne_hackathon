import redis
from dotenv import load_dotenv
import os

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

class Redis(object):
    def __init__(self):
        self.redis = redis.Redis.from_url(REDIS_URL, decode_response=True)