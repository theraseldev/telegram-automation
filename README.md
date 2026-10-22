# MR ADS TG Automation

Advanced Telegram Automation Platform — Flask dashboard + Telethon workers, packaged as a
single deployable web service.

Recovered source of the service running at `probable-tribble-production-c360.up.railway.app`.
Everything needed to build and run is in this repository; secrets and data are **not** —
those live in environment variables and an external PostgreSQL database.

## Contents

| Path | What it is |
|---|---|
| `main.py` | Flask app: dashboard, REST endpoints, background schedulers (~10k lines) |
| `telegram_client.py` | Telethon helpers (broadcast, join, scrape, DMs, reports, health) |
| `templates/` | `dashboard.html`, `admin.html`, `login.html` |
| `requirements.txt`, `runtime.txt` | Python dependencies + runtime pin |
| `railway.toml`, `Procfile`, `.nixpacks/` | Build & start config (NIXPACKS builder) |
| `test_suite.py`, `tests/`, `_evidence/` | Test suite + stored pytest output |
| `*.md` | Feature notes, changelogs, deployment and rollback guides |
| `.env.example` | Every supported environment variable, placeholders only |

## Run locally

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate
# Linux/macOS:  source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # Windows: copy .env.example .env
python main.py
```

Dashboard: <http://localhost:5000> — first run creates `admin` / `admin123` (change it).

## Deploy on Railway

1. Push this repository to GitHub (private repository recommended).
2. Railway → **New Project → Deploy from GitHub repo** and pick this repo.
3. `railway.toml` already configures everything: NIXPACKS builder, start command
   `python main.py`, health check `/ping`, restart on failure.
4. Add these **Variables** in the Railway service (never commit them):

   | Variable | Required | Purpose |
   |---|---|---|
   | `SECRET_KEY` | yes | Flask session signing key (long random string) |
   | `ADMIN_PASSWORD` | yes | dashboard admin password |
   | `DATABASE_URL` | yes | PostgreSQL / Neon connection string (persistent data) |
   | `TELEGRAM_API_ID` | yes | Telegram API credentials |
   | `TELEGRAM_API_HASH` | yes | Telegram API credentials |
   | `BACKGROUND_JOBS_ENABLED` | optional | `0` disables in-process schedulers |
   | `MAX_AUTO_POST_CHANNELS` | optional | channel cap per Auto Video Poster campaign |
   | `MAX_UPLOAD_MB` | optional | max ZIP upload size |
   | `SAVED_MESSAGES_SCAN_LIMIT` | optional | how many Saved Messages to scan |
   | `STALE_TASK_MINUTES` | optional | when a stuck campaign counts as interrupted |
   | `ALLOW_REGISTRATION` | optional | `1` allows public sign-up |
   | `MIGRATE_SQLITE` | optional | import a legacy SQLite file into PostgreSQL |
   | `AUTO_POST_MEDIA_DIR` | optional | override the media directory (default `/data/auto_post_media`) |

5. Attach a **volume mounted at `/data`** so uploaded auto-post media survives redeploys
   (or set `AUTO_POST_MEDIA_DIR` to another persistent path).
6. Deploy.

## Database

`DATABASE_URL` (or `POSTGRES_URL` / `POSTGRESQL_URL` / `NEON_DATABASE_URL`) wins; without it
the app falls back to a local SQLite file and **data is lost on every redeploy**. Missing
tables and columns are created automatically at startup, so pointing at an empty PostgreSQL
database is safe — the schema builds itself.

Telegram accounts are stored as Telethon `StringSession` values in the database, so accounts
survive redeploys as long as the database does.

## Security notes

- Never commit `.env`, a real `DATABASE_URL`, or `*.session` files — `.gitignore` already covers them.
- Telegram session strings in the database are full account credentials: back up dumps privately.
- Change the default `admin123` password in production, and keep the repository private.
