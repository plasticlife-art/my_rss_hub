# MyRssHub

RSS hub for public Telegram channels (+Cineplexx movie listings).
Built for running as a worker container with nginx serving static XML files.

## Features
- Telegram RSS for public channels via `https://t.me/s/<channel>`.
- Cineplexx RSS with diff events (added/removed) + current movies.
- Movie descriptions and sessions (date, time, hall, info).
- Redis cache for film descriptions and sessions.
- Status file: `/out/status.json` with last run results.
- Logs to stdout with run_id correlation.

## Outputs
- `/out/cineplexx_rss.xml`.
- `/out/<telegram_channel>.xml` for each channel.
- `/out/status.json`
- `/out/state_location_<location>.json`

## Quick start (Docker, dev profile)
```bash
cp .env.example app/.env
docker compose --profile dev up -d --build
```

Then open:
- `http://localhost:8080/cineplexx_rss.xml`
- `http://localhost:8080/<channel>.xml`
- `http://localhost:8080/index.html`

## Production (Docker, prod profile)
```bash
WORKER_IMAGE=ghcr.io/plasticlife-art/my_rss_hub:latest \
NGINX_PORT=80 \
docker compose --profile prod up -d --pull always
```

## Configuration
All settings live in `app/.env`. See `.env.example` for defaults.

Key settings:
- `LOCATION` (e.g. `0` or `all`)
- `DATE_MODE` (`today` or `fixed`) + `FIXED_DATE`
- `SCHEDULE_*` (session scraping limits)
- `TELEGRAM_CHANNELS` (comma-separated)
- `REDIS_URL` and `CACHE_ENABLED`
- `INTERVAL_MINUTES` (worker loop)

## Testing
```bash
PYTHONPATH=app/src uv run python -m unittest discover -s app/tests -v
```

## Troubleshooting
- 404 from nginx: check `NGINX_PORT` and that `/out` has XML files.
- Empty schedule: verify `SCHEDULE_ENABLED=true` and `LOCATION` matches site filters.
- Telegram feed empty: ensure channel is public and listed in `TELEGRAM_CHANNELS`.
