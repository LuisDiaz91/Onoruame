import redis
import pickle
import logging
from typing import Optional, Any
from .config import settings

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            decode_responses=False
        )
    
    def get(self, key: str) -> Optional[Any]:
        data = self.client.get(key)
        return pickle.loads(data) if data else None
    
    def set(self, key: str, value: Any, ttl: int = 3600):
        self.client.setex(key, ttl, pickle.dumps(value))
    
    def delete(self, key: str):
        self.client.delete(key)
