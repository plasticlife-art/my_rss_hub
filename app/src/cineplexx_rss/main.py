import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

from .config import load_config
from .scraper import scrape_movies
from .state import load_state, save_state, compute_diff, append_events, update_snapshot
from .rss import build_rss_xml

def resolve_date(cfg) -> str:
    tz = ZoneInfo(cfg.timezone)
    if cfg.date_mode == "fixed":
        if not cfg.fixed_date:
            raise ValueError("DATE_MODE=fixed but FIXED_DATE is empty")
        return cfg.fixed_date
    return datetime.now(tz).date().isoformat()

async def run():
    cfg = load_config()
    tz = ZoneInfo(cfg.timezone)
    now = datetime.now(tz)
    date_str = resolve_date(cfg)

    state_path: Path = cfg.out_dir / f"state_location_{cfg.location}.json"
    rss_path: Path = cfg.out_dir / cfg.rss_filename

    state = load_state(state_path)

    current = await scrape_movies(cfg.base_url, cfg.location, date_str)
    added, removed = compute_diff(state.snapshot, current)

    ts_iso = now.isoformat(timespec="seconds")
    if added or removed:
        append_events(state, added=added, removed=removed, ts_iso=ts_iso, location=cfg.location, date_str=date_str)

    update_snapshot(state, current)
    save_state(state_path, state)

    rss_xml = build_rss_xml(
        title=cfg.feed_title,
        link=cfg.feed_link,
        description=cfg.feed_description,
        now=now,
        events=state.events,
        events_limit=cfg.events_limit,
        current_items=current,
    )
    rss_path.write_text(rss_xml, "utf-8")

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
