from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
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

    return Config(
        base_url=os.getenv("BASE_URL", "https://cineplexx.me").rstrip("/"),
        location=os.getenv("LOCATION", "0"),
        date_mode=os.getenv("DATE_MODE", "today").strip().lower(),
        fixed_date=os.getenv("FIXED_DATE", "").strip(),
        timezone=os.getenv("TIMEZONE", "Europe/Podgorica"),

        out_dir=out_dir,
        rss_filename=os.getenv("RSS_FILENAME", "rss.xml"),
        events_limit=_int("EVENTS_LIMIT", 150),

        feed_title=os.getenv("FEED_TITLE", "Cineplexx — репертуар"),
        feed_link=os.getenv("FEED_LINK", "https://cineplexx.me"),
        feed_description=os.getenv("FEED_DESCRIPTION", "Текущие фильмы в прокате"),
    )
