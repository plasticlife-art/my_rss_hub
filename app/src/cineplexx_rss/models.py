from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class Movie:
    title: str
    url: str

EventType = Literal["add", "remove"]

@dataclass(frozen=True)
class Event:
    type: EventType
    title: str
    url: str
    ts: str          # ISO timestamp when detected
    location: str
    date: str        # date param used when scraping (YYYY-MM-DD)
