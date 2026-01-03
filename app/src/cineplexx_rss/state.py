import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from .models import Movie, Event

@dataclass
class State:
    # Previous snapshot: url -> title
    snapshot: Dict[str, str]
    # Event log (append-only): newest last
    events: List[dict]

def load_state(path: Path) -> State:
    if not path.exists():
        return State(snapshot={}, events=[])
    try:
        data = json.loads(path.read_text("utf-8"))
        return State(
            snapshot=data.get("snapshot", {}) or {},
            events=data.get("events", []) or []
        )
    except Exception:
        return State(snapshot={}, events=[])

def save_state(path: Path, state: State) -> None:
    path.write_text(
        json.dumps({"snapshot": state.snapshot, "events": state.events}, ensure_ascii=False, indent=2),
        "utf-8"
    )

def compute_diff(prev_snapshot: Dict[str, str], current: List[Movie]) -> Tuple[List[Movie], List[Movie]]:
    cur_map = {m.url: m.title for m in current}
    prev_urls = set(prev_snapshot.keys())
    cur_urls = set(cur_map.keys())

    added = [Movie(title=cur_map[u], url=u) for u in sorted(cur_urls - prev_urls)]
    removed = [Movie(title=prev_snapshot[u], url=u) for u in sorted(prev_urls - cur_urls)]
    return added, removed

def append_events(state: State, *, added: List[Movie], removed: List[Movie], ts_iso: str, location: str, date_str: str) -> None:
    # Append in deterministic order (added first, then removed)
    for m in added:
        state.events.append(Event(type="add", title=m.title, url=m.url, ts=ts_iso, location=location, date=date_str).__dict__)
    for m in removed:
        state.events.append(Event(type="remove", title=m.title, url=m.url, ts=ts_iso, location=location, date=date_str).__dict__)

def update_snapshot(state: State, current: List[Movie]) -> None:
    state.snapshot = {m.url: m.title for m in current}
