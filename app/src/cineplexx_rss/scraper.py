import re
from typing import List
from playwright.async_api import async_playwright
from .models import Movie

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

async def scrape_movies(base_url: str, location: str, date_str: str) -> List[Movie]:
    url = f"{base_url}/cinemas?location={location}&date={date_str}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 cineplexx-rss",
            locale="en-US",
        )
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # SPA: wait until film links appear
        await page.wait_for_selector('a[href*="/film/"]', timeout=30000)

        raw = await page.evaluate("""() => {
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
              if (!seen.has(u)) seen.set(u, { title: t, url: u });
            }
            return Array.from(seen.values());
        }""")

        movies = []
        for item in raw:
            title = _normalize_space(item["title"])
            url = item["url"]
            desc = ""
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
                # Prefer specific movie description paragraphs on film pages.
                await page.wait_for_selector(".b-movie-description__text, .b-movie-description", timeout=8000)
                # Expand if the description is collapsed.
                try:
                    btn = page.locator(".b-movie-description__btn")
                    if await btn.count():
                        await btn.first.click()
                        await page.wait_for_timeout(500)
                except Exception:
                    pass
                for _ in range(3):
                    desc = await page.eval_on_selector_all(
                        ".b-movie-description__text",
                        "els => els.map(e => (e.innerText || '').trim()).filter(Boolean).join('\\n\\n')",
                    )
                    if desc:
                        break
                    desc = await page.eval_on_selector(
                        ".b-movie-description",
                        "el => el.innerText || ''",
                    )
                    if desc:
                        break
                    await page.wait_for_timeout(1000)
            except Exception:
                desc = ""

            movies.append(
                Movie(title=title, url=url, description=_normalize_space(desc))
            )

        await browser.close()

    movies = [m for m in movies if m.title and m.url]
    movies.sort(key=lambda m: (m.title.lower(), m.url))
    return movies
