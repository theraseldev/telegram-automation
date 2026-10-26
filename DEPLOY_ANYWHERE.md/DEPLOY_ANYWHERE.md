# Deploy anywhere — all data comes back automatically

Everything that matters lives in **PostgreSQL**, not on the server. Point a new host at
the same database and the app comes back exactly as it was: users, Telegram sessions,
accounts, campaigns, schedules, logs, the Flask secret key and the auto-poster's
Telegram-side video copies.

| Stored in PostgreSQL | Stored on the host (ephemeral) |
|---|---|
| users + password hashes, plans | `auto_post_media/` — ZIP videos uploaded to the Auto Video Poster |
| Telegram accounts **and their session strings** | nothing else |
| broadcasts, DM campaigns, group links, personal chats | |
| every scheduled task (incl. inline tool schedules) | |
| Auto Video Poster campaigns + `vault_state` (Telegram copies of posted videos) | |
| activity log, alerts, proxies, settings | |
| `app_secret_key` (so logins survive restarts) | |

> If `DATABASE_URL` is **not** set, the app silently uses a SQLite file inside the
> container. That file is deleted on every deploy — that is exactly how accounts and
> sessions disappeared before. `/diag` now reports `database.persistent: false` plus a
> `storage_warning` in that case.

## 1. Deploy to a brand-new Railway project

1. Railway → **New Project** → **Deploy from GitHub repo** → `theraseldev/telegram-automation`
   (branch `main`). It builds with the included `railway.toml` / `Procfile`.
2. In the same project: **+ New** → **Database** → **PostgreSQL**.
3. Open the app service → **Variables** → **New Variable**:
   - `DATABASE_URL` = `${{Postgres.DATABASE_URL}}`   ← reference, not a copy
   - `SECRET_KEY` = any long random string (optional — otherwise it is generated once
     and stored in the database)
   - `ADMIN_USERNAME` / `ADMIN_PASSWORD` (optional; defaults are `MRADS` / `MRADS`)
   - `AUTO_POST_MEDIA_DIR` = `/data/auto_post_media` (optional, needs a volume)
4. **Deploy**. On boot the app creates every table, runs the idempotent column
   migrations, seeds the admin account and re-arms interrupted tasks.

Result: a working dashboard with an **empty** database.

## 2. Bring the existing data over

Use the same database on the new host (`DATABASE_URL` → the old database) and there is
nothing else to do. To move the data to a **different** database (e.g. Neon):

```bash
# 1. get a SQL backup (Admin panel → Backup, or the file you already have)
# 2. restore it into the new database
DATABASE_URL="postgresql://user:pass@host/db" python tools/restore_backup.py neon_schema.sql

# target already has data and you really want to replace it:
DATABASE_URL="..." python tools/restore_backup.py neon_schema.sql --force
```

Then point the app at that database and redeploy:

```
DATABASE_URL = postgresql://...neon.tech/neondb?sslmode=require
```

`restore_backup.py` creates missing sequences, skips statements a legacy schema cannot
accept, and prints the row count of the important tables.

## 3. Telegram sessions

Sessions are rows in `telegram_account.session_string` — they are **not** files on the
server, so a new host needs no re-login. They also survive redeploys, as long as the
database is PostgreSQL.

If an account was logged out from Telegram's side it must be re-added; no backup can
revive a revoked session.

## 4. Video files (Auto Video Poster)

ZIP uploads are extracted to `AUTO_POST_MEDIA_DIR`. On a host without a volume that
folder is ephemeral, so:

- attach a Railway **Volume** mounted at `/data` and set
  `AUTO_POST_MEDIA_DIR=/data/auto_post_media`, **or**
- keep the library in Telegram **Saved Messages** and use that as the poster source —
  Telegram keeps the files, so nothing can be lost.

Either way, every video that has been posted once is remembered in
`auto_video_post_campaign.vault_state`, and campaigns keep posting from those Telegram
copies even after the local files disappear.

## 5. Verify a fresh deployment

```
GET /diag
```

Expect:

```json
"database": { "backend": "postgresql", "persistent": true,
              "connected": true, "source_variable": "DATABASE_URL" }
```

`status` is `ok` when the database is reachable and no column is missing. Anything else
(`database_unreachable`, `missing_columns`, `placeholder_secret_key`) is printed with a
`blocking` reason.
