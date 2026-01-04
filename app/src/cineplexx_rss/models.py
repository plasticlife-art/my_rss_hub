from dataclasses import dataclass, field
from typing import Literal

@dataclass(frozen=True)
class Session:
    date: str
    time: str
    hall: str
    info: str
    session_id: str
    cinema_name: str
    purchase_url: str = ""

@dataclass(frozen=True)
class Movie:
    title: str
    url: str
    description: str = ""
    sessions: list[Session] = field(default_factory=list)

EventType = Literal["add", "remove"]

@dataclass(frozen=True)
class Event:
    type: EventType
    title: str
    url: str
    ts: str          # ISO timestamp when detected
    location: str
    date: str        # date param used when scraping (YYYY-MM-DD)
