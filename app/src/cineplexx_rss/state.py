import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any

from .models import Movie, Event

@dataclass
class State:
    # Previous snapshot: url -> {title, first_seen, last_seen}
    snapshot: Dict[str, Dict[str, str]]
    # Event log (append-only): newest last
    events: List[dict]

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_snapshot(raw: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    now_iso = _now_iso()
    normalized: Dict[str, Dict[str, str]] = {}
    for url, value in (raw or {}).items():
        if isinstance(value, dict):
            title = str(value.get("title") or "")
            first_seen = str(value.get("first_seen") or now_iso)
            last_seen = str(value.get("last_seen") or now_iso)
        else:
            title = str(value or "")
            first_seen = now_iso
            last_seen = now_iso
        normalized[url] = {
            "title": title,
            "first_seen": first_seen,
            "last_seen": last_seen,
        }
    return normalized


def load_state(path: Path) -> State:
    logger = logging.getLogger(__name__)
    if not path.exists():
        logger.info("state_load_miss path=%s", path)
        return State(snapshot={}, events=[])
    try:
        data = json.loads(path.read_text("utf-8"))
        snapshot = _normalize_snapshot(data.get("snapshot", {}))
        state = State(
            snapshot=snapshot,
            events=data.get("events", []) or []
        )
        logger.info(
            "state_load_hit path=%s snapshot_size=%s events_total=%s",
            path,
            len(state.snapshot),
            len(state.events),
        )
        return state
    except Exception:
        logger.exception("state_load_failed path=%s", path)
        return State(snapshot={}, events=[])

def save_state(path: Path, state: State) -> None:
    path.write_text(
        json.dumps({"snapshot": state.snapshot, "events": state.events}, ensure_ascii=False, indent=2),
        "utf-8"
    )
    logger = logging.getLogger(__name__)
    logger.info(
        "state_saved path=%s snapshot_size=%s events_total=%s",
        path,
        len(state.snapshot),
        len(state.events),
    )

def compute_diff(prev_snapshot: Dict[str, Dict[str, str]], current: List[Movie]) -> Tuple[List[Movie], List[Movie]]:
    cur_map = {m.url: m.title for m in current}
    prev_urls = set(prev_snapshot.keys())
    cur_urls = set(cur_map.keys())

    added = [Movie(title=cur_map[u], url=u) for u in sorted(cur_urls - prev_urls)]
    removed = [Movie(title=prev_snapshot[u].get("title", ""), url=u) for u in sorted(prev_urls - cur_urls)]
    return added, removed

def append_events(
    state: State,
    *,
    added: List[Movie],
    removed: List[Movie],
    ts_iso: str,
    location: str,
    date_str: str,
    max_events_in_state: int,
) -> None:
    # Append in deterministic order (added first, then removed)
    for m in added:
        state.events.append(Event(type="add", title=m.title, url=m.url, ts=ts_iso, location=location, date=date_str).__dict__)
    for m in removed:
        state.events.append(Event(type="remove", title=m.title, url=m.url, ts=ts_iso, location=location, date=date_str).__dict__)
    if max_events_in_state > 0 and len(state.events) > max_events_in_state:
        events_before = len(state.events)
        trimmed = events_before - max_events_in_state
        state.events = state.events[-max_events_in_state:]
        logging.getLogger(__name__).info(
            "state_events_trimmed events_before=%s events_after=%s trimmed_count=%s max_events_in_state=%s",
            events_before,
            len(state.events),
            trimmed,
            max_events_in_state,
        )

def touch_seen(state: State, url: str, title: str, now_iso: str) -> None:
    existing = state.snapshot.get(url)
    if existing:
        state.snapshot[url]["last_seen"] = now_iso
        if not state.snapshot[url].get("title"):
            state.snapshot[url]["title"] = title
        return
    state.snapshot[url] = {"title": title, "first_seen": now_iso, "last_seen": now_iso}


def get_first_seen(state: State, url: str) -> str:
    existing = state.snapshot.get(url)
    if existing and existing.get("first_seen"):
        return existing["first_seen"]
    now_iso = _now_iso()
    state.snapshot[url] = {"title": "", "first_seen": now_iso, "last_seen": now_iso}
    return now_iso


def update_snapshot(state: State, current: List[Movie], now_iso: str) -> None:
    new_snapshot: Dict[str, Dict[str, str]] = {}
    for m in current:
        existing = state.snapshot.get(m.url)
        if existing:
            first_seen = existing.get("first_seen") or now_iso
        else:
            first_seen = now_iso
        new_snapshot[m.url] = {
            "title": m.title,
            "first_seen": first_seen,
            "last_seen": now_iso,
        }
    state.snapshot = new_snapshot
