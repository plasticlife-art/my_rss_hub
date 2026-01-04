import hashlib
import json
import logging
from typing import Any, Optional

try:
    import redis
except Exception:  # pragma: no cover - fallback when redis isn't installed
    redis = None


class Cache:
    def get_json(self, key: str) -> Optional[dict]:
        raise NotImplementedError

    def set_json(self, key: str, value: dict, ttl_seconds: int) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class NullCache(Cache):
    def get_json(self, key: str) -> Optional[dict]:
        return None

    def set_json(self, key: str, value: dict, ttl_seconds: int) -> None:
        return None

    def close(self) -> None:
        return None


class RedisCache(Cache):
    def __init__(self, redis_url: str, logger: logging.Logger) -> None:
        if redis is None:
            raise RuntimeError("redis package is not available")
        self._logger = logger
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        self._client.ping()

    def get_json(self, key: str) -> Optional[dict]:
        try:
            raw = self._client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            self._logger.warning("cache_get_failed key=%s", key, exc_info=True)
            return None

    def set_json(self, key: str, value: dict, ttl_seconds: int) -> None:
        try:
            payload = json.dumps(value, ensure_ascii=False)
            self._client.setex(key, ttl_seconds, payload)
        except Exception:
            self._logger.warning("cache_set_failed key=%s", key, exc_info=True)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            self._logger.debug("cache_close_failed", exc_info=True)


def cache_key_for_url(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return f"cineplexx:film:{digest}"


def cache_key_for_sessions(url: str, location: str, date: str) -> str:
    raw = f"{url}|{location}|{date}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"cineplexx:sessions:{digest}"


def build_cache(config, logger: logging.Logger) -> Cache:
    if not getattr(config, "cache_enabled", False):
        logger.info("cache_disabled")
        return NullCache()

    redis_url = getattr(config, "redis_url", None)
    if not redis_url:
        logger.warning("cache_enabled_but_no_redis_url")
        return NullCache()

    try:
        logger.info("cache_enabled redis_url=%s", redis_url)
        return RedisCache(redis_url, logger)
    except Exception:
        logger.warning("cache_init_failed using NullCache", exc_info=True)
        return NullCache()
