# SESSION PERSISTENCE GUIDE — Telegram sessions "for life"

**Goal:** create a new Railway service next month (same GitHub repo) and find all
your Telegram accounts already logged in — no re-login, no re-scanning codes.

> Status note: this describes how the shipped code behaves. The behaviour below
> was **not** live-tested against a second Railway service in this session —
> verify once with the checklist in §6.

## 1. Where the sessions actually live

| Item | Stored in | Survives a Railway redeploy / new service? |
|---|---|---|
| Telegram session (`session_string`, Telethon StringSession) | PostgreSQL table `telegram_account`, column `session_string` | **Yes** — as long as the new service uses the same `DATABASE_URL` |
| API ID / API hash | `settings` table | Yes |
| Accounts' proxies | `proxy_config` table | Yes |
| Broadcasts / schedules / campaigns / saved channels | PostgreSQL tables | Yes |
| `*.session` files on disk | **never written** — the code stores the string in the DB only | n/a |
| Railway local filesystem | uploaded ZIP videos, temp files | **No** — ephemeral by design |

So: **the database is your session store.** Railway's disk is deliberately not used
for sessions, because it is wiped on every redeploy.

## 2. The lifetime setup (do this once)

1. Keep the Neon database you already have. Do **not** delete the project or branch.
2. In every new Railway service (same GitHub repo) set these variables to the
   **same values** as the working service:
   - `DATABASE_URL`   ← this is the one that carries the accounts
   - `SECRET_KEY`     ← keeps dashboard logins stable (does not affect Telegram sessions)
   - `ADMIN_PASSWORD` ← so the admin login stays the same
   - optionally `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `MIGRATE_SQLITE=0`
3. Deploy. On boot the app only *adds* missing tables/columns — it never drops or
   rewrites rows, so your `telegram_account` records are found as-is.
4. Open `/login` → Accounts page → every previous account is listed. Nothing to re-add.

## 3. Why a session can still die (Telegram's rules, not the app's)

A Telegram session is valid until **Telegram** decides otherwise:

- the account is logged out of all devices ("terminate all sessions"),
- Telegram flags the login as suspicious / the account gets limited or banned,
- the account's password (2FA) or phone number changes,
- the account stays unused long enough for Telegram to clean up the auth key.

When that happens the stored string is dead and that account must be logged in
again (Accounts page → login flow). **No tool can prevent this** — storing the
session safely is what we can guarantee. Do not treat "session saved" as "will
work forever".

## 4. Backup — the only real insurance

1. **Database level:** use Neon's own snapshots/PITR (or branch) — follow your
   Neon plan's retention. This is the primary backup.
2. **Application level:** while logged in as admin, call
   `GET /bot/export_accounts` and keep the downloaded JSON **offline and
   encrypted**. It contains session strings, which are equivalent to full access
   to those Telegram accounts — treat it like a password list.
   Re-import it into a fresh database with the Accounts page import (or
   `MIGRATE_SQLITE` for an old SQLite file).
3. Verify a backup exists **before** you need it. A backup you never opened is
   not a backup.

## 5. Rules that keep sessions alive

- Never commit `DATABASE_URL`, `.env`, `*.session` or the export JSON to GitHub
  (`.gitignore` in this project already blocks them).
- Never rotate/delete the Neon branch that holds the accounts without exporting first.
- Do not run multiple Railway services against the same database **at the same
  time** with background jobs enabled (`BACKGROUND_JOBS_ENABLED=0` on the extra
  copies), otherwise two schedulers would post/queue the same work.
- If you move to a new Neon project, import the export JSON (or use
  `MIGRATE_SQLITE`) — a fresh empty database obviously has no sessions.

## 6. Verify the setup once (5 minutes)

1. Deploy the new service with the same `DATABASE_URL`.
2. Log in as admin → `GET /bot/db_health` → check
   `counts.telegram_accounts` is the same number as the old service.
3. Accounts page → pick one account → run a harmless action (e.g. read your own
   Saved Messages / open Personal Chats) to prove the session is alive.
4. Broadcast a test message to one private test channel and confirm
   `sent = 1` in the activity log.
5. If an account shows "session invalid / re-login required", only that account
   needs a fresh login.

## 7. Quick reference

```
Sessions live here : PostgreSQL table telegram_account → session_string
Carried by         : DATABASE_URL (same value = same accounts)
Never used for it  : Railway local disk (*.session files are not written)
Primary backup     : Neon snapshots/branches
Portable backup    : GET /bot/export_accounts (JSON, contains secrets — encrypt it)
Kill switches      : Telegram-side revocation, account ban, 2FA change
```
