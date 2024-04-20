import os
from datetime import datetime

from redis import Redis
from dotenv import load_dotenv

load_dotenv()

_STATE_KEY = "etl_state"
_DEFAULT_STATE = "1980-04-01T00:00:00.000000"  # just date from the distant past

redis = Redis(
    host=os.environ.get('REDIS_HOST'),
    port=os.environ.get('REDIS_PORT'),
    db=os.environ.get('REDIS_DB'),
    decode_responses=True
)


def set(key: str, value: datetime) -> None:
    redis.hset(_STATE_KEY, key, value.isoformat())


def get(key: str) -> datetime:
    return datetime.fromisoformat(redis.hget(_STATE_KEY, key) or _DEFAULT_STATE)


def reset(key: str) -> None:
    redis.hdel(_STATE_KEY, key)
