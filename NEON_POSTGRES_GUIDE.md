# Neon PostgreSQL — connection and persistence guide

Goal: every Railway service you create from the same GitHub repo, given the same
`DATABASE_URL`, sees the **same** saved accounts, sessions, schedules, settings
and records.

> Status note: the code paths below are implemented and the URL handling is
> unit-tested. A real Neon connection was **NOT LIVE-TESTED** in this session.

## 1. What is persisted where

| Data | Storage | Survives redeploy/restart |
|---|---|---|
| Telegram accounts + session strings | `telegram_account` table (PostgreSQL) | Yes |
| API ID / API hash | `settings` table | Yes |
| Proxies | `proxy_config` table | Yes |
| Broadcasts, scheduled broadcasts, DM campaigns | PostgreSQL tables + `task_run` | Yes |
| Auto Video Poster campaigns (source, per-channel queues, counters) | `auto_video_post_campaign` (`source_ids`, `queue_state`, `posted_count`, `failed_count`, `skipped_count`, `rounds_completed`) | Yes |
| Saved channels/groups | `saved_target` table | Yes |
| App-level settings (e.g. the generated `SECRET_KEY`) | `global_setting` table | Yes |
| Draft/pending logins (mid-login only) | process memory | No — expected |
| **Uploaded ZIP video files** | local disk (`auto_post_media/`) | **No** — ephemeral. Use the Saved Messages source for restart-safe posting |
| Telegram login session files (`*.session`) | never written outside the DB | n/a |

## 2. Get the connection string
Neon console → your project → **Connection string**. Copy the pooled/`sslmode=require`
form, e.g.:
```
postgresql://USER:PASSWORD@ep-example-123456.us-east-2.aws.neon.tech/neondb?sslmode=require
```
Paste it into Railway → Variables → `DATABASE_URL` (or into `.env` locally).

The app normalises whatever it receives:
- `postgres://…` and `postgresql://…` → `postgresql+psycopg2://…`
- `postgresql+psycopg://…` → `postgresql+psycopg2://…`
- `sslmode=require` is appended when you did not specify it (Neon requires TLS)
- any other scheme (e.g. `sqlite:///…`) is passed through untouched

Accepted variable names, in order: `DATABASE_URL`, `POSTGRES_URL`,
`POSTGRESQL_URL`, `NEON_DATABASE_URL`.

## 3. Driver
`psycopg2-binary==2.9.9` is pinned in `requirements.txt`. If it is missing the
startup log says so explicitly:

> `DATABASE_URL points at PostgreSQL but the psycopg2 driver is not installed.`

Connection options used for PostgreSQL: `pool_pre_ping=True` (drop dead Neon
connections instead of erroring), `pool_recycle=280` (Neon closes idle
connections), `pool_size=5`, `max_overflow=10`.

## 4. Schema creation and migration (safe, idempotent)
On every start `init_app_schema()` runs:
1. `db.create_all()` — creates missing tables only.
2. `run_schema_migration()` — the SQLAlchemy inspector lists existing columns and
   issues `ALTER TABLE … ADD COLUMN …` only for columns that are missing, using
   PostgreSQL types (`BOOLEAN DEFAULT FALSE`) or SQLite types as appropriate.
   Nothing is dropped, renamed or overwritten; running it twice changes nothing
   (proved by `test_schema_migration_adds_missing_column_and_is_idempotent`).
3. Secret key resolution and admin reconciliation.
4. `recover_interrupted_tasks()`.
5. Optional legacy import (below).

If the database is unreachable, the failure is logged with a clear message and
`/bot/db_health` reports `connected: false` — the app does not silently pretend
everything is fine.

## 5. Monthly re-deploy: getting your old data back
1. Railway → New Service → same GitHub repo.
2. Add the **same** `DATABASE_URL`, `SECRET_KEY` and `ADMIN_PASSWORD`.
3. Deploy. The tables already exist, so `create_all()` and the migration are
   no-ops and your accounts, sessions, campaigns and settings appear
   immediately.
4. Verify with `GET /bot/db_health` → `connected: true`, `backend: postgresql`.

## 6. Legacy SQLite → PostgreSQL import (opt-in)
`MIGRATE_SQLITE` controls it:

| Value | Behaviour |
|---|---|
| `0` (default) | Never import. |
| `auto` | Import only when the target database has **no users yet**. |
| `1` | Always attempt the import. |

What it imports: `user` (matched on username), `telegram_account` (matched on
phone), `settings` (API credentials), `proxy_config`, remaining
`scheduled_broadcast` rows. Rules:
- Existing rows are **never** overwritten or deleted.
- Imported rows need an owner: accounts whose `user_id` no longer exists are
  attached to `admin`.
- Only counts are logged — never session strings or API hashes.
- The function returns/does nothing when the target is already SQLite or no
  legacy `telegram_automation.db` file is present on the service.

Put the old `telegram_automation.db` in the repository root of the service and
set `MIGRATE_SQLITE=auto` for the first deploy only, then set it back to `0`.

> The import path is **NOT LIVE-TESTED** — exercise it against a scratch Neon
> database first, and keep a copy of the SQLite file.

## 7. Backups
Use Neon's built-in PITR/branching for snapshots. Application-level backup:
`GET /bot/export_accounts` downloads a JSON file containing your accounts (and
their session strings) plus settings — **treat it like a password** and never
commit it. `.gitignore` already blocks `tgbot_backup*.json` and `*.session`.

## 8. Notes and cautions
- Telegram can revoke a session at any time (user logs out, suspicious-login
  checks, password change). The account then needs a fresh login —
  `/bot/db_health` and the activity log help you spot accounts needing attention.
- Never commit `DATABASE_URL`. Put it only in the environment.
- When you rotate the Neon password, update `DATABASE_URL` in every service.
