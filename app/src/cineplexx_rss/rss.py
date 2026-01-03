from datetime import datetime, timezone
from email.utils import format_datetime
from typing import List, Dict
from xml.sax.saxutils import escape
import hashlib

from .models import Movie

def _event_guid(event: dict) -> str:
    # Stable, unique per event occurrence
    # If the same movie is re-added later, ts changes -> new guid
    raw = f"event:{event.get('type')}|{event.get('url')}|{event.get('ts')}"
    return "urn:sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()

def build_rss_xml(
    *,
    title: str,
    link: str,
    description: str,
    now: datetime,
    # newest last (append-only)
    events: List[dict],
    events_limit: int,
    current_items: List[Movie],
) -> str:
    # RSS 2.0
    pub_date = format_datetime(now)
    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<rss version="2.0">')
    lines.append("<channel>")
    lines.append(f"<title>{escape(title)}</title>")
    lines.append(f"<link>{escape(link)}</link>")
    lines.append(f"<description>{escape(description)}</description>")
    lines.append(f"<lastBuildDate>{escape(pub_date)}</lastBuildDate>")

    # 1) Diff events (most recent first)
    recent_events = list(reversed(events[-max(events_limit, 0):])) if events_limit != 0 else []
    for ev in recent_events:
        et = ev.get("type")
        prefix = "➕ Добавлен: " if et == "add" else "➖ Убран: "
        item_title = prefix + (ev.get("title") or "")
        item_link = ev.get("url") or link

        # pubDate = detection time; fallback to now
        try:
            ev_dt = datetime.fromisoformat(ev.get("ts"))
        except Exception:
            ev_dt = now

        loc = ev.get("location")
        d = ev.get("date")
        schedule_link = f"{link}"  # feed link already points to cinemas?location=...
        item_desc = f"{item_title}\nlocation={loc}, date={d}\n{schedule_link}"

        lines.append("<item>")
        lines.append(f"<title>{escape(item_title)}</title>")
        lines.append(f"<link>{escape(item_link)}</link>")
        lines.append(f"<guid isPermaLink=\"false\">{escape(_event_guid(ev))}</guid>")
        lines.append(f"<pubDate>{escape(format_datetime(ev_dt))}</pubDate>")
        lines.append(f"<description>{escape(item_desc)}</description>")
        lines.append("</item>")

    # 2) Current list (stable GUID = film URL)
    # Use pubDate=now so ordering tends to keep events on top anyway in many readers,
    # but GUID stability prevents re-notifying.
    for m in current_items:
        lines.append("<item>")
        lines.append(f"<title>{escape(m.title)}</title>")
        lines.append(f"<link>{escape(m.url)}</link>")
        lines.append(f"<guid isPermaLink=\"true\">{escape(m.url)}</guid>")
        lines.append(f"<pubDate>{escape(format_datetime(now))}</pubDate>")
        lines.append(f"<description>{escape('Сейчас в репертуаре: ' + m.title)}</description>")
        lines.append("</item>")

    lines.append("</channel>")
    lines.append("</rss>")
    return "\n".join(lines)
