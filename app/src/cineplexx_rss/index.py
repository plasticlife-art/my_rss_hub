from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import format_datetime
from html import escape
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class FeedLink:
    kind: str            # "telegram" | "cineplexx" | ...
    title: str           # Human title
    href: str            # Relative path in /out, e.g. "durov.xml"
    subtitle: str = ""   # Optional: t.me/..., location, etc.


def _fmt_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _fmt_rfc2822(dt: Optional[datetime]) -> str:
    if not dt:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt)


def build_index_html(
    feeds: Iterable[FeedLink],
    site_title: str = "MyRssHub",
    last_updated: Optional[datetime] = None,
    status_href: str = "status.json",
) -> str:
    feeds = list(feeds)

    # Group
    cineplexx = [f for f in feeds if f.kind.lower() == "cineplexx"]
    telegram = [f for f in feeds if f.kind.lower() == "telegram"]
    other = [f for f in feeds if f.kind.lower() not in {"cineplexx", "telegram"}]

    def card(feed: FeedLink) -> str:
        title = escape(feed.title)
        subtitle = escape(feed.subtitle) if feed.subtitle else ""
        href = escape(feed.href)

        # Show subtitle or href as secondary line
        secondary = subtitle or href

        return f"""
        <article class="card" data-title="{escape((feed.title + ' ' + feed.subtitle + ' ' + feed.href).lower())}">
          <div class="card__main">
            <div class="card__title">
              <span class="badge badge--{escape(feed.kind.lower())}">{escape(feed.kind)}</span>
              <a class="link" href="{href}">{title}</a>
            </div>
            <div class="card__sub">{secondary}</div>
          </div>
          <div class="card__actions">
            <a class="btn" href="{href}" title="Open feed">Open</a>
            <button class="btn btn--ghost" type="button" data-copy="{href}" title="Copy link">Copy</button>
          </div>
        </article>
        """

    def section(title: str, items: list[FeedLink], hint: str = "") -> str:
        if not items:
            return f"""
            <section class="section">
              <h2 class="section__title">{escape(title)}</h2>
              <p class="muted">{escape(hint or "No feeds configured.")}</p>
            </section>
            """
        return f"""
        <section class="section">
          <h2 class="section__title">{escape(title)} <span class="count">{len(items)}</span></h2>
          {f'<p class="muted">{escape(hint)}</p>' if hint else ''}
          <div class="grid">
            {''.join(card(i) for i in items)}
          </div>
        </section>
        """

    updated_text = _fmt_dt(last_updated)

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(site_title)}</title>
  <meta name="description" content="RSS feed index" />
  <style>
    :root {{
      --bg: #0b0f14;
      --panel: #121823;
      --panel2: #0f1520;
      --text: #e8edf5;
      --muted: #a8b3c7;
      --line: rgba(255,255,255,.08);
      --shadow: 0 10px 30px rgba(0,0,0,.35);

      --btn: rgba(255,255,255,.08);
      --btnHover: rgba(255,255,255,.13);

      --radius: 16px;
      --max: 1100px;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
    }}

    @media (prefers-color-scheme: light) {{
      :root {{
        --bg: #f7f8fb;
        --panel: #ffffff;
        --panel2: #f2f4f9;
        --text: #0f172a;
        --muted: #475569;
        --line: rgba(2,6,23,.10);
        --shadow: 0 10px 25px rgba(2,6,23,.08);
        --btn: rgba(2,6,23,.06);
        --btnHover: rgba(2,6,23,.10);
      }}
    }}

    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      background: radial-gradient(1200px 600px at 10% 0%, rgba(99,102,241,.22), transparent 60%),
                  radial-gradient(900px 500px at 90% 10%, rgba(34,197,94,.16), transparent 55%),
                  var(--bg);
      color: var(--text);
    }}

    .wrap {{
      max-width: var(--max);
      margin: 0 auto;
      padding: 28px 18px 60px;
    }}

    header {{
      display: flex;
      gap: 16px;
      align-items: flex-start;
      justify-content: space-between;
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 18px 18px;
      position: sticky;
      top: 10px;
      backdrop-filter: blur(10px);
      z-index: 2;
    }}

    .brand {{
      display: grid;
      gap: 6px;
      min-width: 240px;
    }}
    .brand h1 {{
      margin: 0;
      font-size: 20px;
      letter-spacing: .2px;
    }}
    .brand .meta {{
      font-size: 13px;
      color: var(--muted);
    }}
    .brand .meta code {{
      font-family: var(--mono);
      font-size: 12px;
      background: var(--panel2);
      border: 1px solid var(--line);
      padding: 2px 6px;
      border-radius: 999px;
    }}

    .tools {{
      display: flex;
      gap: 10px;
      align-items: center;
      flex: 1;
      justify-content: flex-end;
      min-width: 260px;
    }}

    .search {{
      flex: 1;
      max-width: 520px;
      display: flex;
      gap: 10px;
      align-items: center;
      background: var(--panel2);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 10px 12px;
    }}
    .search input {{
      border: none;
      outline: none;
      background: transparent;
      color: var(--text);
      width: 100%;
      font-size: 14px;
    }}
    .search .hint {{
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }}

    .btn {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: var(--btn);
      color: var(--text);
      text-decoration: none;
      font-size: 13px;
      cursor: pointer;
      user-select: none;
    }}
    .btn:hover {{ background: var(--btnHover); }}
    .btn--ghost {{ background: transparent; }}

    main {{ margin-top: 16px; display: grid; gap: 18px; }}

    .section {{
      background: color-mix(in srgb, var(--panel) 96%, transparent);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 16px;
      box-shadow: var(--shadow);
    }}

    .section__title {{
      margin: 0 0 10px 0;
      font-size: 16px;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .count {{
      font-size: 12px;
      color: var(--muted);
      border: 1px solid var(--line);
      background: var(--panel2);
      padding: 2px 8px;
      border-radius: 999px;
    }}

    .muted {{ margin: 0 0 12px 0; color: var(--muted); font-size: 13px; }}

    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    @media (max-width: 860px) {{
      header {{ flex-direction: column; align-items: stretch; }}
      .tools {{ justify-content: space-between; }}
      .grid {{ grid-template-columns: 1fr; }}
    }}

    .card {{
      border: 1px solid var(--line);
      background: var(--panel2);
      border-radius: 14px;
      padding: 14px;
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }}
    .card__main {{ min-width: 0; display: grid; gap: 6px; }}
    .card__title {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 14px;
      min-width: 0;
    }}
    .link {{
      color: var(--text);
      text-decoration: none;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      display: inline-block;
      max-width: 520px;
    }}
    .link:hover {{ text-decoration: underline; }}
    .card__sub {{
      color: var(--muted);
      font-size: 12px;
      font-family: var(--mono);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 620px;
    }}
    .card__actions {{ display: flex; gap: 8px; flex-shrink: 0; }}

    .badge {{
      font-size: 11px;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,.06);
      text-transform: uppercase;
      letter-spacing: .8px;
      color: var(--muted);
      flex-shrink: 0;
    }}
    .badge--telegram {{ background: rgba(56,189,248,.10); color: color-mix(in srgb, var(--muted) 75%, #38bdf8); }}
    .badge--cineplexx {{ background: rgba(34,197,94,.10); color: color-mix(in srgb, var(--muted) 75%, #22c55e); }}

    footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 12px;
      text-align: center;
    }}

    .toast {{
      position: fixed;
      left: 50%;
      bottom: 18px;
      transform: translateX(-50%);
      background: var(--panel);
      border: 1px solid var(--line);
      padding: 10px 14px;
      border-radius: 999px;
      box-shadow: var(--shadow);
      color: var(--text);
      font-size: 13px;
      opacity: 0;
      pointer-events: none;
      transition: opacity .2s ease;
    }}
    .toast.show {{ opacity: 1; }}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand">
        <h1>{escape(site_title)}</h1>
        <div class="meta">Last updated: <code>{escape(updated_text)}</code> · <a class="link" href="{escape(status_href)}">status.json</a></div>
      </div>

      <div class="tools">
        <div class="search">
          <input id="q" type="search" placeholder="Search feeds (title, channel, filename)..." />
          <span class="hint">⌘/Ctrl + K</span>
        </div>
        <a class="btn" href="{escape(status_href)}" title="Open status.json">Status</a>
      </div>
    </header>

    <main id="content">
      {section("Cineplexx", cineplexx, "Cinema feeds generated by Playwright scraper.")}
      {section("Telegram", telegram, "Public channel feeds parsed from t.me/s/<channel>.")}
      {section("Other", other) if other else ""}
    </main>

    <footer>
      Generated by worker · Served as static files
    </footer>
  </div>

  <div id="toast" class="toast">Copied</div>

  <script>
    (function() {{
      const q = document.getElementById('q');
      const cards = Array.from(document.querySelectorAll('.card'));
      const toast = document.getElementById('toast');

      function showToast(text) {{
        toast.textContent = text;
        toast.classList.add('show');
        setTimeout(() => toast.classList.remove('show'), 900);
      }}

      function filter() {{
        const needle = (q.value || '').trim().toLowerCase();
        for (const c of cards) {{
          const hay = c.getAttribute('data-title') || '';
          c.style.display = (!needle || hay.includes(needle)) ? '' : 'none';
        }}
      }}

      document.addEventListener('keydown', (e) => {{
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {{
          e.preventDefault();
          q.focus();
        }}
      }});

      q.addEventListener('input', filter);

      document.addEventListener('click', async (e) => {{
        const btn = e.target.closest('[data-copy]');
        if (!btn) return;
        const href = btn.getAttribute('data-copy');
        try {{
          // Copy relative link; browsers will paste as-is.
          await navigator.clipboard.writeText(href);
          showToast('Copied: ' + href);
        }} catch {{
          showToast('Copy failed');
        }}
      }});
    }})();
  </script>
</body>
</html>
"""
    return html


def build_index_xml(
    feeds: Iterable[FeedLink],
    site_title: str = "MyRssHub",
    last_updated: Optional[datetime] = None,
    index_href: str = "index.html",
) -> str:
    feeds = list(feeds)
    last_updated_rfc = _fmt_rfc2822(last_updated)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0">',
        "<channel>",
        f"<title>{escape(site_title)}</title>",
        f"<link>{escape(index_href)}</link>",
        f"<description>{escape(site_title)} feeds index</description>",
        f"<lastBuildDate>{escape(last_updated_rfc)}</lastBuildDate>",
    ]

    for f in feeds:
        lines.append("<item>")
        lines.append(f"<title>{escape(f.title)}</title>")
        lines.append(f"<link>{escape(f.href)}</link>")
        lines.append(f"<guid isPermaLink=\"false\">{escape(f.href)}</guid>")
        lines.append(f"<pubDate>{escape(last_updated_rfc)}</pubDate>")
        if f.subtitle:
            lines.append(f"<description>{escape(f.subtitle)}</description>")
        lines.append("</item>")

    lines.append("</channel>")
    lines.append("</rss>")
    return "\n".join(lines)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=encoding)
    tmp.replace(path)
