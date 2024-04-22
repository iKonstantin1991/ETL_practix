from datetime import datetime

from redis import Redis

from etl.settings import settings

_STATE_KEY = "etl_state"

redis = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True
)


def save(key: str, value: datetime) -> None:
    redis.hset(_STATE_KEY, key, value.isoformat())


def get(key: str) -> datetime:
    cached = redis.hget(_STATE_KEY, key)
    return datetime.fromisoformat(cached) if cached else datetime.min


def reset(key: str) -> None:
    redis.hdel(_STATE_KEY, key)
