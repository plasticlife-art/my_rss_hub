import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from time import perf_counter
import time

from .config import load_config
from .scraper import scrape_movies
from .state import load_state, save_state, compute_diff, append_events, update_snapshot
from .rss import build_rss_xml, build_telegram_rss_xml
from .telegram import scrape_telegram_channel
from .logging_utils import setup_logging, new_run_id, set_run_id
from .cache import build_cache
from .index import build_index_html, build_index_xml, FeedLink, atomic_write_text
from .time_utils import format_duration


def resolve_date(cfg) -> str:
    tz = ZoneInfo(cfg.timezone)
    if cfg.date_mode == "fixed":
        if not cfg.fixed_date:
            raise ValueError("DATE_MODE=fixed but FIXED_DATE is empty")
        return cfg.fixed_date
    return datetime.now(tz).date().isoformat()


def _write_status(cfg, payload: dict, logger: logging.Logger) -> None:
    try:
        (cfg.out_dir / "status.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            "utf-8",
        )
    except Exception:
        logger.exception("status_write_failed")


def _load_job_finished_at(path: Path, job_key: str) -> datetime | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text("utf-8"))
    except Exception:
        return None
    job = data.get(job_key) if isinstance(data, dict) else None
    if not isinstance(job, dict):
        return None
    finished = job.get("finished_at")
    if not finished:
        return None
    try:
        parsed = datetime.fromisoformat(str(finished))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _load_status(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text("utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _build_index(cfg, cineplexx_updated: datetime | None, telegram_updated: datetime | None) -> None:
    location_label = "Podgorica" if cfg.location == "0" else cfg.location
    feeds: list[FeedLink] = [
        FeedLink(
            title=f"Cineplexx — {location_label}",
            href=cfg.rss_filename,
            kind="cineplexx",
            subtitle=cfg.feed_description,
        )
    ]
    for channel in cfg.telegram_channels:
        feeds.append(
            FeedLink(
                title=f"Telegram — t.me/{channel}",
                href=f"{channel}.xml",
                kind="telegram",
                subtitle=f"t.me/{channel}",
            )
        )

    last_updated = datetime.now(ZoneInfo(cfg.timezone))
    index_html = build_index_html(
        feeds=feeds,
        site_title="MyRssHub",
        last_updated=last_updated,
        cineplexx_updated=cineplexx_updated,
        telegram_updated=telegram_updated,
    )
    index_xml = build_index_xml(
        feeds=feeds,
        site_title="MyRssHub",
        last_updated=last_updated,
    )
    atomic_write_text(cfg.out_dir / "index.html", index_html)
    atomic_write_text(cfg.out_dir / "index.xml", index_xml)


async def run_cineplexx_job(cfg, logger: logging.Logger, cache) -> dict:
    tz = ZoneInfo(cfg.timezone)
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

    prev_snapshot = state.snapshot
    added, removed = compute_diff(prev_snapshot, current)
    logger.info(
        "diff added_count=%s removed_count=%s state_snapshot_size_before=%s events_total_in_state_before=%s",
        len(added),
        len(removed),
        snapshot_before,
        events_before,
    )
    now = datetime.now(tz)
    now_utc = datetime.now(timezone.utc)
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

    update_snapshot(state, current, now_utc.isoformat())
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
        snapshot_meta=state.snapshot,
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

    return {
        "movies_found": len(current),
        "added": len(added),
        "removed": len(removed),
        "sessions_fetched": sum(len(getattr(movie, "sessions", []) or []) for movie in current),
    }


def run_telegram_job(cfg, logger: logging.Logger) -> dict:
    tz = ZoneInfo(cfg.timezone)
    now = datetime.now(tz)
    telegram_errors = []
    telegram_ok = 0
    telegram_failed = 0

    if cfg.telegram_channels:
        for channel in cfg.telegram_channels:
            logger.info("telegram_scrape_start channel=%s", channel)
            try:
                tg_start = perf_counter()
                tg = scrape_telegram_channel(channel, cfg.telegram_post_limit)
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
                    "content_text": p.text,
                    "images": p.images,
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
                images_mode=cfg.telegram_images_mode,
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

    status = "ok"
    if telegram_failed:
        status = "partial"

    return {
        "status": status,
        "channels_total": len(cfg.telegram_channels),
        "channels_ok": telegram_ok,
        "channels_failed": telegram_failed,
        "errors": telegram_errors,
    }


def main() -> None:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger(__name__)

    cfg = load_config()
    tz = ZoneInfo(cfg.timezone)

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
    logger.info(
        "intervals cineplexx_enabled=%s cineplexx_interval_seconds=%s telegram_enabled=%s telegram_interval_seconds=%s",
        cfg.cineplexx_enabled,
        cfg.cineplexx_interval_seconds,
        cfg.telegram_enabled,
        cfg.telegram_interval_seconds,
    )

    cache = build_cache(cfg, logger)

    next_cineplexx = datetime.now(tz)
    next_telegram = datetime.now(tz)
    status_path = cfg.out_dir / "status.json"

    try:
        while True:
            now = datetime.now(tz)
            cineplexx_due = cfg.cineplexx_enabled and now >= next_cineplexx
            telegram_due = cfg.telegram_enabled and now >= next_telegram

            if not cineplexx_due and not telegram_due:
                next_cineplexx_at = next_cineplexx if cfg.cineplexx_enabled else None
                next_telegram_at = next_telegram if cfg.telegram_enabled else None
                next_run_at = None
                if next_cineplexx_at and next_telegram_at:
                    next_run_at = min(next_cineplexx_at, next_telegram_at)
                else:
                    next_run_at = next_cineplexx_at or next_telegram_at

                if next_run_at is None:
                    sleep_for = 60
                    logger.info("scheduler_idle seconds=%s", sleep_for)
                    time.sleep(sleep_for)
                    continue

                sleep_seconds = next_run_at - now
                sleep_for = max(1, int(sleep_seconds.total_seconds()))
                logger.info(
                    "scheduler_sleep seconds=%s next_cineplexx_run_at=%s next_telegram_run_at=%s",
                    sleep_for,
                    next_cineplexx.isoformat(),
                    next_telegram.isoformat(),
                )
                time.sleep(sleep_for)
                continue

            run_id = new_run_id()
            set_run_id(run_id)

            cineplexx_last: datetime | None = None
            telegram_last: datetime | None = None

            existing_status = _load_status(status_path)
            status_payload = {
                "run_id": run_id,
                "updated_at": now.isoformat(),
                "cineplexx_job": existing_status.get(
                    "cineplexx_job",
                    {"enabled": cfg.cineplexx_enabled, "status": "skipped"},
                ),
                "telegram_job": existing_status.get(
                    "telegram_job",
                    {"enabled": cfg.telegram_enabled, "status": "skipped"},
                ),
            }
            if isinstance(status_payload["cineplexx_job"], dict):
                status_payload["cineplexx_job"]["enabled"] = cfg.cineplexx_enabled
            else:
                status_payload["cineplexx_job"] = {
                    "enabled": cfg.cineplexx_enabled,
                    "status": "skipped",
                }
            if isinstance(status_payload["telegram_job"], dict):
                status_payload["telegram_job"]["enabled"] = cfg.telegram_enabled
            else:
                status_payload["telegram_job"] = {
                    "enabled": cfg.telegram_enabled,
                    "status": "skipped",
                }

            if cineplexx_due:
                job_started = datetime.now(tz)
                logger.info("cineplexx_job_start run_at=%s", job_started.isoformat())
                start_ts = perf_counter()
                cineplexx_status = "ok"
                cineplexx_error = None
                cineplexx_counts = {
                    "movies_found": 0,
                    "added": 0,
                    "removed": 0,
                    "sessions_fetched": 0,
                }
                try:
                    result = asyncio.run(run_cineplexx_job(cfg, logger, cache))
                    cineplexx_counts.update(result)
                except Exception as exc:
                    cineplexx_status = "error"
                    cineplexx_error = {"message": str(exc)}
                    logger.exception("cineplexx_job_failed")

                finished_at = datetime.now(tz)
                duration_seconds = perf_counter() - start_ts
                status_payload["cineplexx_job"] = {
                    "enabled": cfg.cineplexx_enabled,
                    "status": cineplexx_status,
                    "started_at": job_started.isoformat(),
                    "finished_at": finished_at.isoformat(),
                    "duration_seconds": duration_seconds,
                    "duration_human": format_duration(duration_seconds),
                    **cineplexx_counts,
                }
                if cineplexx_error:
                    status_payload["cineplexx_job"]["error"] = cineplexx_error
                logger.info(
                    "cineplexx_job_end status=%s duration_human=%s",
                    cineplexx_status,
                    status_payload["cineplexx_job"]["duration_human"],
                )
                next_cineplexx = finished_at + timedelta(seconds=cfg.cineplexx_interval_seconds)
                status_payload["updated_at"] = datetime.now(tz).isoformat()
                _write_status(cfg, status_payload, logger)
                cineplexx_last = finished_at

            if telegram_due:
                job_started = datetime.now(tz)
                logger.info("telegram_job_start run_at=%s", job_started.isoformat())
                start_ts = perf_counter()
                telegram_status = "ok"
                telegram_error = None
                telegram_counts = {
                    "channels_total": len(cfg.telegram_channels),
                    "channels_ok": 0,
                    "channels_failed": 0,
                }
                try:
                    result = run_telegram_job(cfg, logger)
                    telegram_status = result["status"]
                    telegram_counts.update(
                        {
                            "channels_total": result["channels_total"],
                            "channels_ok": result["channels_ok"],
                            "channels_failed": result["channels_failed"],
                        }
                    )
                    if result["errors"]:
                        telegram_error = {"message": result["errors"][0]["message"]}
                except Exception as exc:
                    telegram_status = "error"
                    telegram_error = {"message": str(exc)}
                    logger.exception("telegram_job_failed")

                finished_at = datetime.now(tz)
                duration_seconds = perf_counter() - start_ts
                status_payload["telegram_job"] = {
                    "enabled": cfg.telegram_enabled,
                    "status": telegram_status,
                    "started_at": job_started.isoformat(),
                    "finished_at": finished_at.isoformat(),
                    "duration_seconds": duration_seconds,
                    "duration_human": format_duration(duration_seconds),
                    **telegram_counts,
                }
                if telegram_error:
                    status_payload["telegram_job"]["error"] = telegram_error
                logger.info(
                    "telegram_job_end status=%s duration_human=%s",
                    telegram_status,
                    status_payload["telegram_job"]["duration_human"],
                )
                next_telegram = finished_at + timedelta(seconds=cfg.telegram_interval_seconds)
                status_payload["updated_at"] = datetime.now(tz).isoformat()
                _write_status(cfg, status_payload, logger)
                telegram_last = finished_at

            if cineplexx_due or telegram_due:
                if cineplexx_last is None:
                    cineplexx_last = _load_job_finished_at(status_path, "cineplexx_job")
                if telegram_last is None:
                    telegram_last = _load_job_finished_at(status_path, "telegram_job")
                _build_index(cfg, cineplexx_updated=cineplexx_last, telegram_updated=telegram_last)

            now = datetime.now(tz)
            next_cineplexx_at = next_cineplexx if cfg.cineplexx_enabled else None
            next_telegram_at = next_telegram if cfg.telegram_enabled else None
            next_run_at = None
            if next_cineplexx_at and next_telegram_at:
                next_run_at = min(next_cineplexx_at, next_telegram_at)
            else:
                next_run_at = next_cineplexx_at or next_telegram_at

            if next_run_at is None:
                sleep_for = 60
                logger.info("scheduler_idle seconds=%s", sleep_for)
                time.sleep(sleep_for)
                continue

            sleep_seconds = next_run_at - now
            sleep_for = max(1, int(sleep_seconds.total_seconds()))
            logger.info(
                "scheduler_sleep seconds=%s next_cineplexx_run_at=%s next_telegram_run_at=%s",
                sleep_for,
                next_cineplexx.isoformat(),
                next_telegram.isoformat(),
            )
            time.sleep(sleep_for)
    finally:
        if cache is not None:
            try:
                cache.close()
            except Exception:
                logger.debug("cache_close_failed", exc_info=True)


if __name__ == "__main__":
    main()
