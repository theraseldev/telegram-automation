# Railway Deployment Guide — MR ADS TG Automation

> Status note: this guide documents the configuration shipped in the project.
> The deployment itself was **NOT LIVE-TESTED** in this session.

## 1. Prerequisites
- GitHub repository containing this project (secrets must **not** be committed).
- A Neon PostgreSQL database (see `NEON_POSTGRES_GUIDE.md`).
- Railway account.

## 2. Create the service
1. Railway → **New Project** → **Deploy from GitHub repo** → select this repository.
2. Railway auto-detects Python (Nixpacks). `railway.toml` in the repo already sets:
   - `startCommand = "python main.py"`
   - `healthcheckPath = "/ping"`, `healthcheckTimeout = 120`
   - `restartPolicyType = "ON_FAILURE"`, `restartPolicyMaxRetries = 10`
3. `Procfile` contains `web: python main.py` as a fallback for other builders.
4. `runtime.txt` pins Python `3.11.9`.

**Do not** switch the start command to Gunicorn with multiple workers: the
background schedulers are single-process by design, and multiple workers would
run each scheduler more than once.

## 3. Environment variables (Railway → Variables)

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | **Yes** | Neon connection string. `postgres://` and `postgresql://` are both accepted; `?sslmode=require` is added automatically. |
| `SECRET_KEY` | **Yes** | Long random string for session signing. If omitted, a key is generated and stored in the database (sessions survive restarts only if the DB is reachable at boot). |
| `ADMIN_PASSWORD` | **Yes** | Admin login password. If it does not match the stored hash, the app **syncs** the stored password to this value on startup — this is the documented password-recovery path. |
| `MAX_AUTO_POST_CHANNELS` | optional | Channels per Auto Video Poster campaign (default `50`). Shared by the UI and backend. |
| `MAX_UPLOAD_MB` | optional | Upload size cap in MB (default `500`). |
| `ALLOW_REGISTRATION` | optional | `1` enables public sign-up. Default `0` (admin-created accounts only). |
| `BACKGROUND_JOBS_ENABLED` | optional | `0` disables all schedulers in this process. |
| `STALE_TASK_MINUTES` | optional | Minutes before a stuck task is treated as interrupted (default `30`). |
| `SAVED_MESSAGES_SCAN_LIMIT` | optional | How many Saved Messages to scan for a Saved Messages campaign (default `800`). |
| `MIGRATE_SQLITE` | optional | `0` (default), `1`, or `auto` — see `NEON_POSTGRES_GUIDE.md`. |

Generate a secret key:
```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

## 4. Networking
- Railway injects `PORT`; the app reads it (`_env_int('PORT', 5000)`) and binds
  `0.0.0.0`. No manual port configuration is needed.
- `GET /ping` returns the health payload Railway probes.
- First boot creates the schema, runs the column migration, resolves the secret
  key and ensures the admin account. Logs print the backend and the data source.

## 5. Restart / redeploy behaviour (implemented, not live-tested)
- Tables and columns are created idempotently on every boot; existing rows are never dropped.
- Tasks left `running` are marked `interrupted`; Auto Poster `posting` rounds are
  re-armed and their queue positions are read back from the database, so a video
  is never posted twice.
- Scheduled broadcasts that were killed are re-queued as `pending` when nothing
  was confirmed as sent, otherwise marked `interrupted`.
- **Uploaded ZIP videos live on the ephemeral disk and are gone after a
  redeploy.** Affected campaigns are set to `paused` with `needs_reupload` and a
  clear message. For restart-proof posting, use the **Saved Messages** source,
  because the videos stay in Telegram.
- If Telegram revokes a session, that account must be logged in again from the
  Accounts page — this is expected Telegram behaviour, not an app bug.

## 6. Post-deploy smoke checklist
1. `https://<app>/ping` responds.
2. Log in at `/login` with `admin` / `ADMIN_PASSWORD`.
3. `GET /bot/db_health` (while logged in) shows `connected: true` and
   `backend: postgresql`.
4. Accounts page → add an account (API ID + hash + phone + login code).
5. Broadcast a single message to one test channel; confirm the counters
   (`sent` / `failed` / `skipped`) match reality.
6. Auto Video Poster → Saved Messages source, 1 channel, 1-minute timers;
   confirm `posted_count` increases and the log shows the target channel.
7. Redeploy the service and confirm accounts, campaigns and settings are still
   there (proof that Neon persistence works).

## 7. Troubleshooting
- **`psycopg2` missing / driver error** — `psycopg2-binary==2.9.9` must be in
  `requirements.txt` (it is). Check the build log.
- **Database initialization failed** — the log line names the source variable.
  Verify the Neon string, that the database exists, and that `sslmode=require`
  is reachable. The app still boots and reports the failure through
  `/bot/db_health` so the dashboard is not a blank crash.
- **429 / rate limits** — Telegram controls these. The app waits the required
  time and records the target as `skipped`; it never bypasses the limit.
- **Login loop** — `SECRET_KEY` changed between deploys. Set it explicitly as
  an environment variable.
- **Upload fails with 413** — raise `MAX_UPLOAD_MB` or split the ZIP files.
