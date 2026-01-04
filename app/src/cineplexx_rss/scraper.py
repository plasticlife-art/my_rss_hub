import asyncio
import logging
import re
from datetime import datetime, timedelta
from time import perf_counter
from typing import List
from playwright.async_api import async_playwright
from .models import Movie, Session
from .cache import cache_key_for_url, cache_key_for_sessions, Cache

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

async def _cache_get(cache: Cache, key: str) -> dict | None:
    try:
        return await asyncio.to_thread(cache.get_json, key)
    except Exception:
        logging.getLogger(__name__).warning("cache_get_failed key=%s", key, exc_info=True)
        return None


async def _cache_set(cache: Cache, key: str, value: dict, ttl_seconds: int) -> None:
    try:
        await asyncio.to_thread(cache.set_json, key, value, ttl_seconds)
    except Exception:
        logging.getLogger(__name__).warning("cache_set_failed key=%s", key, exc_info=True)


async def scrape_movies(
    base_url: str,
    location: str,
    date_str: str,
    cache: Cache,
    film_cache_ttl_seconds: int,
    cache_negative_ttl_seconds: int,
    max_film_pages_concurrency: int,
    schedule_enabled: bool,
    schedule_max_days_ahead: int,
    schedule_max_sessions_per_movie: int,
    schedule_max_dates_per_movie: int,
    schedule_concurrency: int,
    schedule_cache_ttl_seconds: int,
    schedule_cache_negative_ttl_seconds: int,
) -> List[Movie]:
    url = f"{base_url}/cinemas?location={location}&date={date_str}"
    logger = logging.getLogger(__name__)
    logger.info("cineplexx_scrape_start url=%s location=%s date=%s", url, location, date_str)
    start_ts = perf_counter()
    cache_hits = 0
    cache_misses = 0
    film_pages_fetched = 0
    semaphore = asyncio.Semaphore(max_film_pages_concurrency)
    schedule_semaphore = asyncio.Semaphore(schedule_concurrency)
    schedule_cache_hits = 0
    schedule_cache_misses = 0
    dates_probed = 0
    dates_with_sessions = 0
    sessions_found = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 cineplexx-rss",
            locale="en-US",
        )
        try:
            start_date = datetime.fromisoformat(date_str).date()
        except Exception:
            start_date = datetime.utcnow().date()
        date_list = [
            (start_date + timedelta(days=offset)).isoformat()
            for offset in range(schedule_max_days_ahead + 1)
        ]

        async def fetch_movie_list(list_date: str) -> list[dict]:
            list_url = f"{base_url}/cinemas?location={location}&date={list_date}"
            page = await context.new_page()
            try:
                await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                # SPA: wait until film links appear
                await page.wait_for_selector('a[href*="/film/"]', timeout=30000)
                return await page.evaluate("""() => {
                    const anchors = Array.from(document.querySelectorAll('a[href*="/film/"]'));
                    const seen = new Map();
                    for (const a of anchors) {
                      const href = a.getAttribute('href') || '';
                      if (!href.includes('/film/')) continue;

                      const textCandidates = [
                        a.innerText,
                        a.getAttribute('aria-label'),
                        a.getAttribute('title'),
                        a.querySelector('[data-title]')?.getAttribute('data-title'),
                        a.querySelector('.movie-title,.movie__title,.film-title,.film__title')?.innerText,
                        a.querySelector('img')?.getAttribute('alt'),
                        a.querySelector('img')?.getAttribute('title'),
                      ];

                      let t = '';
                      for (const c of textCandidates) {
                        if (!c) continue;
                        const s = String(c).trim();
                        if (s.length >= 2) { t = s; break; }
                      }

                      if (!t || t.length < 2) continue;

                      const u = href.startsWith('http') ? href : (location.origin + href);
                      const base = u.split('?')[0];
                      if (!seen.has(base)) seen.set(base, { title: t, url: base });
                    }
                    return Array.from(seen.values());
                }""")
            except Exception:
                logger.warning(
                    "cineplexx_movie_list_failed date=%s url=%s",
                    list_date,
                    list_url,
                    exc_info=True,
                )
                return []
            finally:
                await page.close()

        if schedule_enabled:
            raw_items: list[dict] = []
            for list_date in date_list:
                raw_items.extend(await fetch_movie_list(list_date))
            if raw_items:
                unique = {item["url"]: item for item in raw_items if item.get("url")}
                raw = list(unique.values())
            else:
                raw = []
            logger.info("cineplexx_movie_list_dates=%s movies_found=%s", len(date_list), len(raw))
        else:
            raw = await fetch_movie_list(date_str)

        async def fetch_description(film_url: str) -> str:
            nonlocal film_pages_fetched
            async with semaphore:
                film_pages_fetched += 1
                film_page = await context.new_page()
                desc = ""
                try:
                    await film_page.goto(film_url, wait_until="networkidle", timeout=60000)
                    # Dismiss cookie overlay if present; it blocks clicks.
                    try:
                        await film_page.evaluate("""() => {
                            const ids = ["CybotCookiebotDialog", "CybotCookiebotDialogBodyUnderlay"];
                            for (const id of ids) {
                                const el = document.getElementById(id);
                                if (el) el.remove();
                            }
                            document.body.style.overflow = "auto";
                        }""")
                    except Exception:
                        pass
                    # Prefer specific movie description paragraphs on film pages.
                    await film_page.wait_for_selector(".b-movie-description__text, .b-movie-description", timeout=8000)
                    # Expand if the description is collapsed.
                    try:
                        btn = film_page.locator(".b-movie-description__btn")
                        if await btn.count():
                            try:
                                await film_page.evaluate("""() => {
                                    const ids = ["CybotCookiebotDialog", "CybotCookiebotDialogBodyUnderlay"];
                                    for (const id of ids) {
                                        const el = document.getElementById(id);
                                        if (el) el.remove();
                                    }
                                    document.body.style.overflow = "auto";
                                }""")
                            except Exception:
                                pass
                            await btn.first.click()
                            await film_page.wait_for_timeout(500)
                    except Exception:
                        pass
                    for _ in range(3):
                        desc = await film_page.eval_on_selector_all(
                            ".b-movie-description__text",
                            "els => els.map(e => (e.innerText || '').trim()).filter(Boolean).join('\\n\\n')",
                        )
                        if desc:
                            break
                        desc = await film_page.eval_on_selector(
                            ".b-movie-description",
                            "el => el.innerText || ''",
                        )
                        if desc:
                            break
                        await film_page.wait_for_timeout(1000)
                except Exception:
                    desc = ""
                finally:
                    await film_page.close()
                return _normalize_space(desc)

        async def fetch_sessions_for_date(film_url: str, session_date: str) -> list[dict]:
            nonlocal schedule_cache_hits, schedule_cache_misses, dates_probed, dates_with_sessions, sessions_found
            base_url = film_url.split("?", 1)[0]
            page_url = f"{base_url}?date={session_date}&location={location}"
            cache_key = cache_key_for_sessions(base_url, location, session_date)
            cached = await _cache_get(cache, cache_key)
            if cached is not None:
                schedule_cache_hits += 1
                sessions = cached.get("sessions") or []
                if sessions:
                    dates_with_sessions += 1
                    sessions_found += len(sessions)
                return sessions

            schedule_cache_misses += 1
            async with schedule_semaphore:
                dates_probed += 1
                film_page = await context.new_page()
                sessions: list[dict] = []
                try:
                    await film_page.goto(page_url, wait_until="networkidle", timeout=60000)
                    sessions = await film_page.evaluate("""() => {
                        const items = Array.from(document.querySelectorAll('li[data-session-id]'));
                        const out = [];
                        for (const li of items) {
                          const sessionId = li.getAttribute('data-session-id') || '';
                          const timeEl = li.querySelector('p.l-tickets__item-time');
                          const hallEl = li.querySelector('p.l-tickets__item-cinema');
                          const infoEls = Array.from(li.querySelectorAll('p.l-tickets__item-info'));
                          let info = '';
                          for (const el of infoEls) {
                            const t = (el.innerText || '').trim();
                            if (t) { info = t; break; }
                          }
                          let purchase = '';
                          const linkEl = li.querySelector('a[href]');
                          if (linkEl) purchase = linkEl.getAttribute('href') || '';
                          if (purchase.startsWith('/')) purchase = location.origin + purchase;
                          if (purchase.startsWith('//')) purchase = 'https:' + purchase;

                          let cinemaName = '';
                          const dataWrap = li.closest('div[id^="data-"]');
                          if (dataWrap && dataWrap.id) {
                            cinemaName = dataWrap.id.replace(/^data-/, '').replace(/-/g, ' ').trim();
                          }
                          if (!cinemaName) {
                            const titleEl = document.querySelector('a.b-entity-content__title, a.b-entity-content__link');
                            if (titleEl) cinemaName = (titleEl.innerText || '').trim();
                          }
                          out.push({
                            session_id: sessionId,
                            time: timeEl ? (timeEl.innerText || '').trim() : '',
                            hall: hallEl ? (hallEl.innerText || '').trim() : '',
                            info,
                            cinema_name: cinemaName,
                            purchase_url: purchase,
                          });
                        }
                        return out;
                    }""")
                except Exception:
                    sessions = []
                finally:
                    await film_page.close()

            if sessions:
                dates_with_sessions += 1
                sessions_found += len(sessions)
                await _cache_set(
                    cache,
                    cache_key,
                    {
                        "sessions": sessions,
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                    },
                    schedule_cache_ttl_seconds,
                )
            else:
                await _cache_set(
                    cache,
                    cache_key,
                    {
                        "sessions": [],
                        "error": "no_sessions",
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                    },
                    schedule_cache_negative_ttl_seconds,
                )
            return sessions

        async def build_movie(item: dict) -> Movie:
            nonlocal cache_hits, cache_misses
            title = _normalize_space(item.get("title", ""))
            film_url = item.get("url", "")
            if not film_url:
                return Movie(title=title, url="", description="", sessions=[])

            cache_key = cache_key_for_url(film_url)
            cached = await _cache_get(cache, cache_key)
            if cached:
                desc = cached.get("description") or ""
                cached_title = cached.get("title") or title
                if desc or cached.get("error"):
                    cache_hits += 1
                    return Movie(title=cached_title or title, url=film_url, description=desc or "")

            cache_misses += 1
            desc = await fetch_description(film_url)
            if desc:
                await _cache_set(
                    cache,
                    cache_key,
                    {
                        "title": title,
                        "description": desc,
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                        "source": "cineplexx",
                    },
                    film_cache_ttl_seconds,
                )
            else:
                logger.warning("movie_description_missing url=%s", film_url)
                await _cache_set(
                    cache,
                    cache_key,
                    {
                        "title": title,
                        "description": None,
                        "error": "not_found",
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                        "source": "cineplexx",
                    },
                    cache_negative_ttl_seconds,
                )
            sessions: list[Session] = []
            if schedule_enabled:
                date_sessions = await asyncio.gather(
                    *(fetch_sessions_for_date(film_url, d) for d in date_list)
                )
                total_sessions = 0
                for idx, session_date in enumerate(date_list):
                    if total_sessions >= schedule_max_sessions_per_movie:
                        break
                    raw_sessions = date_sessions[idx]
                    if not raw_sessions:
                        continue
                    if len(sessions) and len({s.date for s in sessions}) >= schedule_max_dates_per_movie:
                        break
                    for raw in raw_sessions:
                        if total_sessions >= schedule_max_sessions_per_movie:
                            break
                        sessions.append(
                            Session(
                                date=session_date,
                                time=raw.get("time", ""),
                                hall=raw.get("hall", ""),
                                info=raw.get("info", ""),
                                session_id=raw.get("session_id", ""),
                                cinema_name=raw.get("cinema_name", ""),
                                purchase_url=raw.get("purchase_url", ""),
                            )
                        )
                        total_sessions += 1

            return Movie(title=title, url=film_url, description=desc, sessions=sessions)

        tasks = [build_movie(item) for item in raw]
        movies = await asyncio.gather(*tasks)

        await browser.close()

    movies = [m for m in movies if m.title and m.url]
    movies.sort(key=lambda m: (m.title.lower(), m.url))
    duration_ms = int((perf_counter() - start_ts) * 1000)
    logger.info(
        "cineplexx_scrape_done duration_ms=%s movies_found=%s cache_hits=%s cache_misses=%s film_pages_fetched=%s schedule_enabled=%s schedule_cache_hits=%s schedule_cache_misses=%s dates_probed=%s dates_with_sessions=%s sessions_found=%s",
        duration_ms,
        len(movies),
        cache_hits,
        cache_misses,
        film_pages_fetched,
        schedule_enabled,
        schedule_cache_hits,
        schedule_cache_misses,
        dates_probed,
        dates_with_sessions,
        sessions_found,
    )
    return movies
