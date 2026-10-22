# Rollback / Backup Instructions

## 1. Before you deploy this version (do this first)

1. **Database snapshot**
   - Neon: create a branch or a point-in-time snapshot from the Neon console.
     This is the authoritative backup of accounts, session strings, campaigns
     and settings.
   - Local SQLite: copy `telegram_automation.db` somewhere safe.
2. **Repository backup**
   ```bash
   git checkout -b pre-audit-backup
   git add -A && git commit -m "backup before v3.0.0 audit"
   git push origin pre-audit-backup
   ```
   The original project archive you supplied (`MR_ADS_TG_ORIGINAL_FULL_PROJECT_PLUS_ACCIO_AUDIT_PROMPT.zip`)
   is kept unchanged in the delivery package under `original/` as a byte-level backup.
3. **Credentials backup** — copy the current Railway variables
   (`DATABASE_URL`, `SECRET_KEY`, `ADMIN_PASSWORD`, …) into a password manager.
   `SECRET_KEY` matters: changing it logs every user out (nothing is lost).
4. **Account export** (optional) — `GET /bot/export_accounts` while logged in as
   admin. Contains session strings: store it encrypted, never in Git.

## 2. What this version changes in the database (all additive)

- New tables: `task_run`, `saved_target`.
- New columns: `user.is_banned`, `user.last_login_at`, and 11 columns on
  `auto_video_post_campaign` (`source_type`, `source_ids`, `queue_state`,
  `posted_count`, `failed_count`, `skipped_count`, `rounds_completed`,
  `stop_requested`, `needs_reupload`, `last_round_summary`, `worker_heartbeat`).
- **No table, row or column is dropped, renamed or rewritten.** The migration is
  `ALTER TABLE … ADD COLUMN`, guarded by a column-existence check.

Because the change is additive, rolling back the application image while leaving
the database as-is is safe: the older code simply ignores the extra columns.

## 3. Rollback procedure

### 3a. Application-only rollback (most common)
1. Railway → your service → **Deployments** → pick the previous successful
   deployment → **Redeploy**.
   Or: `git revert <merge-commit> && git push` (Railway rebuilds automatically).
2. No database action needed — old code ignores the new columns/tables.
3. Verify: `/ping`, then login, then `GET /bot/db_health`.

### 3b. Database rollback (only if you must)
Restore the Neon snapshot/branch taken in step 1 and point `DATABASE_URL` at it.
Telegram session strings are user-generated secrets: if you restore an older
snapshot, accounts created afterwards must be re-added.

### 3c. Full rollback to the original project
1. Restore the repository from `pre-audit-backup` (or unzip the `original/`
   folder from the delivery package).
2. Set Railway's start command back to `python main.py` (unchanged) and redeploy.
3. **Do not** delete the new tables/columns: if you ever redeploy this fixed
   version, it will keep working and your newer data is still intact.

## 4. Operational backup rhythm (recommended)
- **Daily**: Neon PITR is automatic; confirm retention covers your needs.
- **Weekly**: `GET /bot/export_accounts` (encrypted, offline storage).
- **Before every schema-affecting upgrade**: Neon branch + repo branch, as above.
- **Never** commit: `.env`, `*.session`, `*.db`, `tgbot_backup*.json` —
  `.gitignore` in this project already excludes all of them.

## 5. Recovery checks after any rollback
1. `/ping` responds.
2. Login works (`admin` / `ADMIN_PASSWORD`).
3. `/bot/db_health` → `connected: true`, `backend: postgresql`.
4. Accounts page lists your accounts; a test broadcast reaches one test channel.
5. Activity log shows new entries (proves writes are landing).
