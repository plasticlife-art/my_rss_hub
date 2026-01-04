import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from time import perf_counter

from .config import load_config
from .scraper import scrape_movies
from .state import load_state, save_state, compute_diff, append_events, update_snapshot
from .rss import build_rss_xml, build_telegram_rss_xml
from .telegram import scrape_telegram_channel
from .logging_utils import setup_logging, new_run_id, set_run_id
from .cache import build_cache


def resolve_date(cfg) -> str:
    tz = ZoneInfo(cfg.timezone)
    if cfg.date_mode == "fixed":
        if not cfg.fixed_date:
            raise ValueError("DATE_MODE=fixed but FIXED_DATE is empty")
        return cfg.fixed_date
    return datetime.now(tz).date().isoformat()


async def run(cfg, logger: logging.Logger, cache) -> dict:
    tz = ZoneInfo(cfg.timezone)
    now = datetime.now(tz)
    date_str = resolve_date(cfg)

    state_path: Path = cfg.out_dir / f"state_location_{cfg.location}.json"
    rss_path: Path = cfg.out_dir / cfg.rss_filename

    state = load_state(state_path)
    snapshot_before = len(state.snapshot)
    events_before = len(state.events)

    current = await scrape_movies(
        cfg.base_url,
        cfg.location,
        date_str,
        cache,
        cfg.film_cache_ttl_seconds,
        cfg.cache_negative_ttl_seconds,
        cfg.max_film_pages_concurrency,
        cfg.schedule_enabled,
        cfg.schedule_max_days_ahead,
        cfg.schedule_max_sessions_per_movie,
        cfg.schedule_max_dates_per_movie,
        cfg.schedule_concurrency,
        cfg.schedule_cache_ttl_seconds,
        cfg.schedule_cache_negative_ttl_seconds,
    )

    added, removed = compute_diff(state.snapshot, current)
    logger.info(
        "diff added_count=%s removed_count=%s state_snapshot_size_before=%s events_total_in_state_before=%s",
        len(added),
        len(removed),
        snapshot_before,
        events_before,
    )

    ts_iso = now.isoformat(timespec="seconds")
    if added or removed:
        append_events(
            state,
            added=added,
            removed=removed,
            ts_iso=ts_iso,
            location=cfg.location,
            date_str=date_str,
            max_events_in_state=cfg.max_events_in_state,
        )

    update_snapshot(state, current)
    save_state(state_path, state)
    logger.info(
        "state_snapshot_size_after=%s events_total_in_state_after=%s",
        len(state.snapshot),
        len(state.events),
    )

    logger.info("rss_write_start path=%s", rss_path)
    rss_start = perf_counter()
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
    rss_duration = perf_counter() - rss_start
    events_in_rss = (
        list(reversed(state.events[-max(cfg.events_limit, 0):]))
        if cfg.events_limit != 0
        else []
    )
    logger.info(
        "rss_write_end duration_ms=%s items_count=%s path=%s",
        int(rss_duration * 1000),
        len(events_in_rss) + len(current),
        rss_path,
    )

    telegram_errors = []
    telegram_ok = 0
    telegram_failed = 0

    if cfg.telegram_channels:
        for channel in cfg.telegram_channels:
            logger.info("telegram_scrape_start channel=%s", channel)
            try:
                tg_start = perf_counter()
                tg = await asyncio.to_thread(
                    scrape_telegram_channel,
                    channel,
                    cfg.telegram_post_limit,
                )
                tg_duration = perf_counter() - tg_start
            except Exception as exc:
                telegram_failed += 1
                telegram_errors.append(
                    {"scope": f"telegram:{channel}", "message": str(exc)}
                )
                logger.exception("telegram_scrape_failed channel=%s", channel)
                continue

            items = [
                {
                    "title": p.title,
                    "url": p.url,
                    "description": p.description,
                    "published": p.published,
                    "guid": p.url,
                }
                for p in tg.posts
            ]
            tg_rss = build_telegram_rss_xml(
                title=tg.title,
                link=f"https://t.me/{channel}",
                description=tg.description,
                now=now,
                items=items,
            )
            tg_path = cfg.out_dir / f"{channel}.xml"
            tg_path.write_text(tg_rss, "utf-8")
            telegram_ok += 1
            logger.info(
                "telegram_scrape_end channel=%s duration_ms=%s items_found=%s path=%s",
                channel,
                int(tg_duration * 1000),
                len(items),
                tg_path,
            )

    return {
        "cineplexx": {
            "movies_found": len(current),
            "added": len(added),
            "removed": len(removed),
        },
        "telegram": {
            "channels_total": len(cfg.telegram_channels),
            "channels_ok": telegram_ok,
            "channels_failed": telegram_failed,
        },
        "errors": telegram_errors,
    }


def main() -> None:
    run_id = new_run_id()
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    set_run_id(run_id)
    logger = logging.getLogger(__name__)

    started_at = datetime.now(timezone.utc)
    start_ts = perf_counter()
    status = "ok"
    errors = []
    cineplexx_counts = {"movies_found": 0, "added": 0, "removed": 0}
    telegram_counts = {"channels_total": 0, "channels_ok": 0, "channels_failed": 0}
    error_exc = None
    cfg = None
    cache = None

    try:
        cfg = load_config()
        logger.info(
            "cache_config enabled=%s ttl_seconds=%s negative_ttl_seconds=%s max_concurrency=%s redis_url=%s",
            cfg.cache_enabled,
            cfg.film_cache_ttl_seconds,
            cfg.cache_negative_ttl_seconds,
            cfg.max_film_pages_concurrency,
            cfg.redis_url or "",
        )
        logger.info(
            "schedule_config enabled=%s max_days_ahead=%s max_sessions_per_movie=%s max_dates_per_movie=%s concurrency=%s",
            cfg.schedule_enabled,
            cfg.schedule_max_days_ahead,
            cfg.schedule_max_sessions_per_movie,
            cfg.schedule_max_dates_per_movie,
            cfg.schedule_concurrency,
        )
        cache = build_cache(cfg, logger)
        logger.info(
            "run_start location=%s date_mode=%s out_dir=%s events_limit=%s max_movies=%s telegram_channels_count=%s",
            cfg.location,
            cfg.date_mode,
            cfg.out_dir,
            cfg.events_limit,
            "none",
            len(cfg.telegram_channels),
        )
        result = asyncio.run(run(cfg, logger, cache))
        cineplexx_counts = result["cineplexx"]
        telegram_counts = result["telegram"]
        errors.extend(result["errors"])
        if telegram_counts["channels_failed"] > 0:
            status = "partial"
    except Exception as exc:
        status = "error"
        errors.append({"scope": "cineplexx", "message": str(exc)})
        logger.exception("run_failed")
        error_exc = exc
    finally:
        finished_at = datetime.now(timezone.utc)
        duration_ms = int((perf_counter() - start_ts) * 1000)
        status_payload = {
            "run_id": run_id,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": duration_ms,
            "status": status,
            "cineplexx": cineplexx_counts,
            "telegram": telegram_counts,
            "errors": errors[:20],
        }
        if cfg is not None:
            try:
                (cfg.out_dir / "status.json").write_text(
                    json.dumps(status_payload, ensure_ascii=False, indent=2),
                    "utf-8",
                )
            except Exception:
                logger.exception("status_write_failed")
        if cache is not None:
            try:
                cache.close()
            except Exception:
                logger.debug("cache_close_failed", exc_info=True)
        logger.info(
            "run_end status=%s duration_ms=%s movies_found=%s added=%s removed=%s telegram_ok=%s telegram_failed=%s",
            status,
            duration_ms,
            cineplexx_counts["movies_found"],
            cineplexx_counts["added"],
            cineplexx_counts["removed"],
            telegram_counts["channels_ok"],
            telegram_counts["channels_failed"],
        )

    if error_exc is not None:
        raise error_exc


if __name__ == "__main__":
    main()
