# CHANGELOG.md — MR ADS TG AUTOMATION

## [3.5.0] — 2026-10-09 — Auto Video Poster integrity + dead buttons

Applied **on top of** the current `main` (which already carries the
`vault_state` / persistent-media work); nothing from those commits was reverted.
Five defects found by a full static + test audit of this tree:

### Fixed — Auto Video Poster

- **Duplicate posting.** The scheduler re-arms any campaign left in `posting`
  whose heartbeat is older than 5 minutes. A round that is legitimately slow
  looks identical, so a **second round thread** was started for the same
  campaign and posted from the same queue. `main.py` now keeps an in-process
  registry (`_AVP_ACTIVE`) of running rounds; a campaign already being posted is
  never started twice, and the stale-heartbeat recovery skips campaigns with a
  live thread.
- **Pause was silently undone.** The tail of a round (and its `except` handler)
  wrote `status = 'running'` unconditionally. The tail now commits the counters,
  **re-reads the row** and only resumes a campaign the operator did not pause
  (a paused campaign gets `next_run_at = None`). The `except` handler refreshes
  before deciding and never resurrects a paused/deleted campaign — it used to
  bring a paused campaign back to `running`.
- `AUTO_POST_MAX_CONCURRENT` was defined but never used — now enforced.

### Fixed — frontend

- **`toggleAllGroupsSched()` did not exist** although the Scheduled Broadcasts
  "Select All" checkbox called it (`ReferenceError`, dead button). Implemented.
- **`collectCheckedDestinations()` used the group row text** (`"Title1,234"` —
  title + member count) instead of the checkbox `value`, so any schedule built
  from a group checkbox list had destinations Telegram cannot resolve. It now
  prefers the checkbox `value`.

- **In-tool scheduling was broken on 5 of 7 tool pages.** `INLINE_PAGES` pointed
  at element ids that exist nowhere in the markup (`broadcastMessage`,
  `bcastGroups`, `broadcastGroupsList`, `campaignMessage(s)`, `grplinkMessage`,
  `grplinkLinks`, `schedMessage`, `schedTargets`, `channelAccount/Message/Targets`,
  `chpost*`), so the "send now or schedule" panel answered *"Add at least one
  destination in the form above on this page"* on every page except Bulk DMs, no
  matter what the operator filled in. The panel now uses the real ids, reads
  checkbox-list destinations through `destSelector`, reads DM Campaign messages
  from `#msgTemplates .msg-template`, uses the open chat for Personal Chats, and
  understands a multi-select account. It also renders two **optional override
  boxes** ("Destinations", "Message") plus a **"👁 Check what will be sent"**
  button, so the panel can never dead-end: if a page exposes nothing readable the
  operator types it into the panel. The heading now names the task type it
  creates (e.g. `⏱ Group Link to Message — send now or schedule`). A missing
  account is reported instead of sending with none.

### Verified

- `152 passed in 69.92s` (`python -m pytest tests/test_suite.py -q`, Telegram
  faked) with these changes applied.
- `main.py` parses; `node --check` on every `<script>` block and inline handler of
  all three templates: **0 failures** (4 blocks, 123 handlers).
- **Browser check** against the running app (login → dashboard → Bulk DMs /
  Group Broadcast / Scheduled Broadcasts): the delivery card shows the new title,
  both override textareas and the "Check what will be sent" button on all three
  pages, the button's handler runs and renders its alert, and there are **zero
  console errors**. Dashboard renders 30 nav items; Auto Video Poster page renders
  clean.
- Note: the local instance had no connected Telegram account, so the browser check
  could not exercise a *filled* form (group lists and message boxes stay empty).
  Destination resolution from real form values is covered by the template-contract
  audit, not by a live click.

## [3.4.0] — 2026-10-09 — production bug found on the live PostgreSQL database

Root cause confirmed from the live Railway logs of the `probable-tribble` service:

```
WARNING: Schema migration skipped user.is_banned:
  (psycopg2.errors.SyntaxError) syntax error at or near "user"
ERROR:   login: database error:
  (psycopg2.errors.UndefinedColumn) column user.is_banned does not exist
```

### Fixed
- **`main.py` — `_migration_ddl()` (new) + `run_schema_migration()`**: PostgreSQL
  reserves the word `user`, so the unquoted `ALTER TABLE user ADD COLUMN
  is_banned ...` raised a syntax error. Those columns were therefore never
  created, and every ORM query for a user failed with
  `column user.is_banned does not exist`, making `POST /login` answer 503.
  Identifiers are now quoted on PostgreSQL (`ALTER TABLE "user" ADD COLUMN
  "is_banned" BOOLEAN DEFAULT FALSE`) while SQLite keeps the plain form.
- A failed migration step is now logged at **ERROR** (it was a warning, which is
  why the fault was easy to miss in the Railway log).
- **`_fallback_user_lookup()` (new)**: if an ORM user query fails because a
  column is missing, login/`load_user` fall back to a raw-SQL lookup of the core
  columns (`id, username, password`) against `"user"` / `user`. Schema drift can
  no longer lock the administrator out. A genuinely unreachable database still
  returns an actionable 503.
- `init_app_schema()`: the secret-key, admin and recovery steps are now isolated —
  one failing step no longer stops the others.
- `/diag`: new `database.missing_columns` list and a `schema_incomplete` status,
  so a column that the migration could not add is visible in the browser.

### Tests
- 132 passed (previous 129 + `test_migration_ddl_quotes_postgres_reserved_words`,
  `test_login_survives_a_missing_column_on_the_database`,
  `test_diag_reports_missing_columns_and_ok_status`; the "database down" test now
  simulates a truly unreachable database).

## [3.0.0] — 2026-10-09

Full project audit + repair + UI redesign. Backwards compatible: no existing route, form id, JavaScript handler or database record was removed.

### Added
- `main.py`
  - PostgreSQL/Neon support: `_normalize_db_url()`, `_resolve_database_uri()`, `DB_URI`/`DB_SOURCE`/`DB_IS_POSTGRES`, engine options `pool_pre_ping`/`pool_recycle`.
  - `run_schema_migration()` — engine-aware, inspector-based, idempotent column migration (replaces the SQLite-only `migrate_db()`).
  - `init_app_schema()` — runs for `python main.py` **and** WSGI imports; creates tables, migrates, resolves the secret key, ensures the admin, recovers interrupted work.
  - `import_legacy_sqlite()` — opt-in (`MIGRATE_SQLITE=0|1|auto`), never overwrites existing rows.
  - `resolve_secret_key()` — env key, else a generated key persisted in `GlobalSetting`.
  - `recover_interrupted_tasks()` — re-arms stuck `TaskRun`, Auto Poster (`posting`) and Scheduled Broadcast (`running`) records.
  - Safe input helpers: `safe_int`, `safe_float`, `safe_bool`, `clean_str`, `clean_str_list`, `parse_datetime_utc`, `dedupe_preserve`, `redact`, `parse_channel_target`, `friendly_telegram_error`, `next_repeat_settings`.
  - Durable progress: `task_run_start/update/finish/snapshot` + new `TaskRun` model.
  - New models/columns: `TaskRun`, `SavedTarget`, `User.is_banned`, `User.last_login_at`, and 11 new `AutoVideoPostCampaign` columns (`source_type`, `source_ids`, `queue_state`, `posted_count`, `failed_count`, `skipped_count`, `rounds_completed`, `stop_requested`, `needs_reupload`, `last_round_summary`, `worker_heartbeat`).
  - Concurrency guards: `get_account_lock()`, `account_busy()`, `start_job_once()`.
  - New routes: `GET /bot/app_config`, `GET /bot/db_health`, `GET/POST /bot/saved_targets`, `DELETE /bot/saved_targets/<id>`.
  - Global JSON error handlers for 413 / 500 / unknown `/bot/*` 404.
  - Template context processor exposing `registration_enabled` and `max_auto_post_channels`.
- `requirements.txt` — `psycopg2-binary`, plus the previously missing runtime imports (`requests`, `pysocks`, `duckduckgo-search`, `SpeechRecognition`, `gTTS`, `pydub`).
- `.env.example`, `.gitignore`, `tests/test_suite.py`, `_evidence/pytest.txt`.
- Docs: `AUDIT_REPORT.md`, `CHANGELOG.md`, `TEST_RESULTS.txt`, `RAILWAY_DEPLOYMENT_GUIDE.md`, `NEON_POSTGRES_GUIDE.md`, `ROLLBACK_BACKUP_GUIDE.md`.

### Changed
- `main.py` — Auto Video Poster rewritten end to end: ZIP **and** Telegram Saved Messages sources; per-channel shuffled queues persisted in the DB; real `posted/failed/skipped` counters; per-channel error isolation; FloodWait wait-then-retry; `send_file` return value checked before advancing; shared `MAX_AUTO_POST_CHANNELS` limit; `stop_requested` cooperation on delete.
- `main.py` — messaging routes: `broadcast`, `schedule_broadcast`, `run_scheduled_broadcasts`, `send_dm`, `send_dm_multi`, `dm_campaign`, `campaign_status`, `task_status`, `send_personal_message`, `get_chat_history`, `forward_message`, `forward_messages`, `get_scheduled_broadcasts` — safe parsing, dedupe, up-front validation, honest statuses, durable progress, per-account locking.
- `main.py` — `login()` blocks banned users, records `last_login_at`; `register()` gated by `ALLOW_REGISTRATION`; `export_accounts` adds a warning field + audit log; `dashboard_stats` returns `success`.
- `railway.toml` — added `healthcheckPath = "/ping"` and `healthcheckTimeout`.
- `templates/dashboard.html` — new design system (CSS v3, ~250 lines), responsive layout (mobile drawer, tablet/desktop grids), toasts, empty/error states, progress shimmer, badges; Auto Video Poster panel (live channel counter, save/load channels, real counters, per-campaign errors); duplicate forwarder IDs fixed.
- `templates/login.html` — rewritten: modern dark card, inline messages, double-submit guard, Register tab hidden when registration is disabled.
- `templates/admin.html` — rewritten: modern dark design **and** corrected endpoints (`/bot/admin/*`), ban/unban, last login, empty/loading states.

### Removed
- `migrate_db()` (replaced by `run_schema_migration()`), the SQLite-only migration list, and the root-level duplicate `dashboard.html` (the active template is `templates/dashboard.html`; two copies invited editing the wrong one). `attached_assets/` scratch files were also dropped.

### Security
- No secret is hardcoded any more (secret key persisted in the DB when the env var is missing).
- Public registration disabled by default; admin routes verify the admin identity; ban flag is real.
- Error responses never echo session strings, `api_hash` or `DATABASE_URL`.
- Sessions/DB/backup files excluded from Git via `.gitignore`.

### Fixed
- 45+ `int(None)` → 500 crash sites.
- Finite `repeat_count` turning into an endless loop.
- Zero-confirmed-send campaigns being reported as successful.
- `is_banned` never persisting; duplicates being messaged twice; two campaigns sharing one account.
- Broken Admin Panel endpoints; unpopulated Forwarder account selector; `int(group_id)` and `dialog.entity.bot` crashes; corrupt `groups` JSON breaking the scheduled list.
