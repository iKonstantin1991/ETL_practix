import os
from datetime import datetime

from redis import Redis
from dotenv import load_dotenv

load_dotenv()

_STATE_KEY = "etl_state"

redis = Redis(
    host=os.environ.get('REDIS_HOST'),
    port=os.environ.get('REDIS_PORT'),
    db=os.environ.get('REDIS_DB'),
    decode_responses=True
)


def save(key: str, value: datetime) -> None:
    redis.hset(_STATE_KEY, key, value.isoformat())


def get(key: str) -> datetime:
    cached = redis.hget(_STATE_KEY, key)
    return datetime.fromisoformat(cached) if cached else datetime.min


def reset(key: str) -> None:
    redis.hdel(_STATE_KEY, key)
