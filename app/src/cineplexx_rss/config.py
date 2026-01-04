from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
import logging
import os

load_dotenv()

@dataclass(frozen=True)
class Config:
    base_url: str
    location: str
    date_mode: str
    fixed_date: str
    timezone: str

    out_dir: Path
    rss_filename: str
    events_limit: int
    max_events_in_state: int
    telegram_channels: list[str]
    telegram_post_limit: int
    redis_url: str | None
    cache_enabled: bool
    film_cache_ttl_seconds: int
    cache_negative_ttl_seconds: int
    max_film_pages_concurrency: int
    schedule_enabled: bool
    schedule_max_days_ahead: int
    schedule_max_sessions_per_movie: int
    schedule_max_dates_per_movie: int
    schedule_concurrency: int
    schedule_cache_ttl_seconds: int
    schedule_cache_negative_ttl_seconds: int

    feed_title: str
    feed_link: str
    feed_description: str

def load_config() -> Config:
    out_dir = Path(os.getenv("OUT_DIR", "./out"))
    out_dir.mkdir(parents=True, exist_ok=True)

    def _int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    def _bool(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None or raw.strip() == "":
            return default
        val = raw.strip().lower()
        if val in ("1", "true", "yes", "on"):
            return True
        if val in ("0", "false", "no", "off"):
            return False
        logging.getLogger(__name__).warning(
            "invalid %s=%s, using default=%s",
            name,
            raw,
            default,
        )
        return default

    def _list(name: str) -> list[str]:
        raw = os.getenv(name, "")
        items = [x.strip() for x in raw.split(",") if x.strip()]
        return items

    max_events_in_state = _int("MAX_EVENTS_IN_STATE", 5000)
    if max_events_in_state <= 0:
        logging.getLogger(__name__).warning(
            "invalid MAX_EVENTS_IN_STATE=%s, using default=5000",
            os.getenv("MAX_EVENTS_IN_STATE", ""),
        )
        max_events_in_state = 5000

    redis_url = os.getenv("REDIS_URL", "").strip() or None
    cache_enabled = _bool("CACHE_ENABLED", bool(redis_url))
    film_cache_ttl_seconds = _int("CINEPLEXX_FILM_CACHE_TTL_SECONDS", 604800)
    if film_cache_ttl_seconds <= 0:
        logging.getLogger(__name__).warning(
            "invalid CINEPLEXX_FILM_CACHE_TTL_SECONDS=%s, using default=604800",
            os.getenv("CINEPLEXX_FILM_CACHE_TTL_SECONDS", ""),
        )
        film_cache_ttl_seconds = 604800
    cache_negative_ttl_seconds = _int("CINEPLEXX_CACHE_NEGATIVE_TTL_SECONDS", 3600)
    if cache_negative_ttl_seconds <= 0:
        logging.getLogger(__name__).warning(
            "invalid CINEPLEXX_CACHE_NEGATIVE_TTL_SECONDS=%s, using default=3600",
            os.getenv("CINEPLEXX_CACHE_NEGATIVE_TTL_SECONDS", ""),
        )
        cache_negative_ttl_seconds = 3600
    max_film_pages_concurrency = _int("MAX_FILM_PAGES_CONCURRENCY", 4)
    if max_film_pages_concurrency < 1:
        logging.getLogger(__name__).warning(
            "invalid MAX_FILM_PAGES_CONCURRENCY=%s, using default=4",
            os.getenv("MAX_FILM_PAGES_CONCURRENCY", ""),
        )
        max_film_pages_concurrency = 4

    schedule_enabled = _bool("SCHEDULE_ENABLED", True)
    schedule_max_days_ahead = _int("SCHEDULE_MAX_DAYS_AHEAD", 14)
    if schedule_max_days_ahead <= 0:
        logging.getLogger(__name__).warning(
            "invalid SCHEDULE_MAX_DAYS_AHEAD=%s, using default=14",
            os.getenv("SCHEDULE_MAX_DAYS_AHEAD", ""),
        )
        schedule_max_days_ahead = 14
    schedule_max_sessions_per_movie = _int("SCHEDULE_MAX_SESSIONS_PER_MOVIE", 50)
    if schedule_max_sessions_per_movie <= 0:
        logging.getLogger(__name__).warning(
            "invalid SCHEDULE_MAX_SESSIONS_PER_MOVIE=%s, using default=50",
            os.getenv("SCHEDULE_MAX_SESSIONS_PER_MOVIE", ""),
        )
        schedule_max_sessions_per_movie = 50
    schedule_max_dates_per_movie = _int("SCHEDULE_MAX_DATES_PER_MOVIE", 10)
    if schedule_max_dates_per_movie <= 0:
        logging.getLogger(__name__).warning(
            "invalid SCHEDULE_MAX_DATES_PER_MOVIE=%s, using default=10",
            os.getenv("SCHEDULE_MAX_DATES_PER_MOVIE", ""),
        )
        schedule_max_dates_per_movie = 10
    schedule_concurrency = _int("SCHEDULE_CONCURRENCY", 4)
    if schedule_concurrency < 1:
        logging.getLogger(__name__).warning(
            "invalid SCHEDULE_CONCURRENCY=%s, using default=4",
            os.getenv("SCHEDULE_CONCURRENCY", ""),
        )
        schedule_concurrency = 4
    schedule_cache_ttl_seconds = _int("SCHEDULE_CACHE_TTL_SECONDS", 21600)
    if schedule_cache_ttl_seconds <= 0:
        logging.getLogger(__name__).warning(
            "invalid SCHEDULE_CACHE_TTL_SECONDS=%s, using default=21600",
            os.getenv("SCHEDULE_CACHE_TTL_SECONDS", ""),
        )
        schedule_cache_ttl_seconds = 21600
    schedule_cache_negative_ttl_seconds = _int("SCHEDULE_CACHE_NEGATIVE_TTL_SECONDS", 3600)
    if schedule_cache_negative_ttl_seconds <= 0:
        logging.getLogger(__name__).warning(
            "invalid SCHEDULE_CACHE_NEGATIVE_TTL_SECONDS=%s, using default=3600",
            os.getenv("SCHEDULE_CACHE_NEGATIVE_TTL_SECONDS", ""),
        )
        schedule_cache_negative_ttl_seconds = 3600

    return Config(
        base_url=os.getenv("BASE_URL", "https://cineplexx.me").rstrip("/"),
        location=os.getenv("LOCATION", "0"),
        date_mode=os.getenv("DATE_MODE", "today").strip().lower(),
        fixed_date=os.getenv("FIXED_DATE", "").strip(),
        timezone=os.getenv("TIMEZONE", "Europe/Podgorica"),

        out_dir=out_dir,
        rss_filename=os.getenv("RSS_FILENAME", "cineplexx_rss.xml"),
        events_limit=_int("EVENTS_LIMIT", 150),
        max_events_in_state=max_events_in_state,
        telegram_channels=_list("TELEGRAM_CHANNELS"),
        telegram_post_limit=_int("TELEGRAM_POST_LIMIT", 5),
        redis_url=redis_url,
        cache_enabled=cache_enabled,
        film_cache_ttl_seconds=film_cache_ttl_seconds,
        cache_negative_ttl_seconds=cache_negative_ttl_seconds,
        max_film_pages_concurrency=max_film_pages_concurrency,
        schedule_enabled=schedule_enabled,
        schedule_max_days_ahead=schedule_max_days_ahead,
        schedule_max_sessions_per_movie=schedule_max_sessions_per_movie,
        schedule_max_dates_per_movie=schedule_max_dates_per_movie,
        schedule_concurrency=schedule_concurrency,
        schedule_cache_ttl_seconds=schedule_cache_ttl_seconds,
        schedule_cache_negative_ttl_seconds=schedule_cache_negative_ttl_seconds,

        feed_title=os.getenv("FEED_TITLE", "Cineplexx — репертуар"),
        feed_link=os.getenv("FEED_LINK", "https://cineplexx.me"),
        feed_description=os.getenv("FEED_DESCRIPTION", "Текущие фильмы в прокате"),
    )
