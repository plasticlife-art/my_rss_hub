import logging
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import List, Dict
from xml.sax.saxutils import escape
import hashlib

from .models import Movie, Session

def _format_sessions(sessions: List[Session]) -> str:
    if not sessions:
        return ""
    lines = ["Sessions:"]
    for s in sessions:
        parts = [s.date, s.time, s.hall, s.info]
        line = " — ".join([p for p in parts if p])
        if s.purchase_url:
            line = f"{line} — {s.purchase_url}"
        lines.append(line)
    return "\n".join(lines)

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
    logger = logging.getLogger(__name__)
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
        item_desc = m.description.strip() if m.description else ""
        if not item_desc:
            item_desc = "Сейчас в репертуаре: " + m.title
        sessions_block = _format_sessions(m.sessions)
        if sessions_block:
            item_desc = item_desc + "\n\n" + sessions_block
        lines.append(f"<description>{escape(item_desc)}</description>")
        lines.append("</item>")

    lines.append("</channel>")
    lines.append("</rss>")
    rss_xml = "\n".join(lines)
    logger.debug(
        "cineplexx_rss_built events_count=%s current_count=%s",
        len(events),
        len(current_items),
    )
    return rss_xml


def build_telegram_rss_xml(
    *,
    title: str,
    link: str,
    description: str,
    now: datetime,
    items: List[dict],
) -> str:
    logger = logging.getLogger(__name__)
    pub_date = format_datetime(now)
    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<rss version="2.0">')
    lines.append("<channel>")
    lines.append(f"<title>{escape(title)}</title>")
    lines.append(f"<link>{escape(link)}</link>")
    lines.append(f"<description>{escape(description)}</description>")
    lines.append(f"<lastBuildDate>{escape(pub_date)}</lastBuildDate>")

    for item in items:
        item_title = item.get("title") or "Post"
        item_link = item.get("url") or link
        item_desc = item.get("description") or ""
        item_guid = item.get("guid") or item_link
        try:
            item_dt = datetime.fromisoformat(item.get("published") or "")
        except Exception:
            item_dt = now

        lines.append("<item>")
        lines.append(f"<title>{escape(item_title)}</title>")
        lines.append(f"<link>{escape(item_link)}</link>")
        lines.append(f"<guid isPermaLink=\"true\">{escape(item_guid)}</guid>")
        lines.append(f"<pubDate>{escape(format_datetime(item_dt))}</pubDate>")
        if item_desc:
            lines.append(f"<description>{escape(item_desc)}</description>")
        lines.append("</item>")

    lines.append("</channel>")
    lines.append("</rss>")
    rss_xml = "\n".join(lines)
    logger.debug("telegram_rss_built items_count=%s", len(items))
    return rss_xml
