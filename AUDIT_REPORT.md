# AUDIT_REPORT.md — MR ADS TG Automation

Full audit + repair pass. Every status below states **exactly** what was verified.

**Verification legend**
- **PASS** — covered by an automated test in `tests/test_suite.py` that actually ran.
- **PASS (static)** — verified by parsing source/templates (route contract, duplicate IDs, JS syntax), not by a live call.
- **NOT LIVE-TESTED** — needs a real Telegram account / Railway / Neon environment. Logic was reviewed and refactored, but not executed against the live service.

**Automated test result: `115 passed in 46.92s`** (raw output in `_evidence/pytest.txt`).

---

## 1. Critical bugs found and fixed

| # | Problem (evidence) | Fix | Status |
|---|---|---|---|
| 1 | `DATABASE_URL` was **never read**; the app always used a local SQLite file. Nothing could persist to Neon. | `_resolve_database_uri()` + `_normalize_db_url()` read `DATABASE_URL`/`POSTGRES_URL`/`NEON_DATABASE_URL`, convert `postgres://` → `postgresql+psycopg2://`, force `sslmode=require`, enable `pool_pre_ping` + `pool_recycle`. | PASS (`test_database_uri_normalisation`, `test_db_health_reports_backend_without_secrets`) |
| 2 | `migrate_db()` opened the **local SQLite file directly**, so on PostgreSQL it did nothing — new columns never appeared. | `run_schema_migration()` uses the SQLAlchemy inspector + `ALTER TABLE ... ADD COLUMN` with dialect-correct types. Runs on every startup. | PASS (`test_schema_migration_adds_missing_column_and_is_idempotent`, `test_new_columns_exist_on_the_live_schema`) |
| 3 | Standalone **Admin Panel was completely broken**: JS called `/api/admin/*`, backend serves `/bot/admin/*` → every table/button 404. | `admin.html` rewritten to call `/bot/admin/*` via one `ADMIN_API` constant; response keys matched to the real routes. | PASS (static: `test_admin_panel_calls_only_existing_endpoints`) |
| 4 | Duplicate `id="fwdAccount"` / `id="fwdSource"` on two pages → the Forwarder page's own inputs were unreachable and the account `<select>` was never populated. | Forwarder page IDs renamed to `fwdFwdAccount` / `fwdFwdSource`; `startForward()` and both account-population lists updated. | PASS (`test_dashboard_has_no_duplicate_element_ids`) |
| 5 | `User.is_banned` existed only as a raw `ALTER TABLE` column — the ORM did not know it, so **bans never persisted** and re-login was not blocked. | Real model column added; ban enforced in `login()`; `Admin → Users` shows Ban/Unban and last login. | PASS (`test_ban_flag_actually_persists`, `test_banned_user_cannot_log_in`) |
| 6 | 45+ call sites did `int(data.get('x'))`. `None`/`''`/`'abc'` → **`TypeError: int() argument ... not 'NoneType'` → HTTP 500**. | `safe_int()` / `safe_float()` / `safe_bool()` / `clean_str()` / `parse_datetime_utc()` applied across all routes; each now returns a clear 400 validation error. | PASS (2×10 parametrized tests: empty body and `None` values) |
| 7 | Campaign reported **`completed` even when every send failed**; FloodWait targets were silently dropped. | Honest outcomes `completed` / `partial` / `failed` (zero confirmed sends can never be "completed"); FloodWait waits the required time and reports the target as `skipped` with a reason. | PASS (broadcast success/partial/failed/skipped tests) |
| 8 | `repeat_count` bug: a finite count (e.g. 3) decremented to 0, and 0 meant "∞" → **a finite schedule became an endless loop**. | `next_repeat_settings()`: `0 = ∞` (unchanged UI contract), `N = N runs in total`. | PASS (`test_next_repeat_settings`, 6 cases) |
| 9 | Scheduled broadcast stuck in `running` after a crash never ran again. | `recover_interrupted_tasks()`: `sent_count > 0` → `interrupted`, otherwise re-queued as `pending`. | PASS (`test_recover_interrupted_tasks_requeues_scheduled_broadcasts`) |
| 10 | `except Exception: pass` in the broadcast/scheduler paths hid every failure. | All swallowed exceptions now log/collect a reason; per-target errors surface in the API. | PASS (`test_broadcast_partial_failure_is_not_reported_as_success`) |
| 11 | No duplicate protection: the same username/group could receive a message twice. | `dedupe_preserve()` on every recipient list + verification up-front. | PASS (`test_broadcast_deduplicates_recipients`, `test_dm_campaign_deduplicates_and_persists_progress`) |
| 12 | Two campaigns could run on one account simultaneously. | Per-account lock (`get_account_lock`) + `409` when busy. | PASS (`test_second_campaign_on_a_busy_account_is_rejected`) |
| 13 | In-memory `task_progress` died on restart → progress lost. | Durable `TaskRun` table; status endpoints fall back to it. | PASS (`test_progress_survives_process_memory_loss`) |
| 14 | `/register` was public → anyone could create a dashboard account. | Gated by `ALLOW_REGISTRATION` (default `0` → 403 with a clear message). Admin can still create users. | PASS (`test_public_registration_is_disabled`) |
| 15 | `SECRET_KEY` had a hardcoded default. | Env value wins; otherwise a random key is generated and **persisted in the database** so sessions survive restarts. | PASS (startup path exercised by every test) |
| 16 | No `.gitignore` → `.db` files and session strings could be committed. | `.gitignore` blocks `*.session`, `*.db`, `.env`, `tgbot_backup*.json`, `auto_post_media/`. | PASS (static) |
| 17 | Error responses leaked internals / returned HTML where JSON was expected. | Global handlers: JSON 413 (upload limit), JSON 500 without secrets, JSON 404 for `/bot/*`. | PASS (`test_upload_limit_returns_json_413`, `test_unexpected_error_handler_returns_json`, `test_error_responses_never_leak_secrets`) |
| 18 | `dashboard_stats` returned no `success` key (inconsistent contract). | `success: True/False` added. | PASS (static + route tests) |

## 1b. Bug found on the LIVE PostgreSQL database (2026-10-09, v3.4.0)

This one was only reproducible against a real PostgreSQL/Neon database — the
SQLite test run could never have caught it.

| Symptom (observed live) | Root cause (from the Railway log) | Fix | Status |
|---|---|---|---|
| `/login` answered **503 "The database is not reachable yet"** for every password, on a healthy database | PostgreSQL reserves the word `user`; the migration ran `ALTER TABLE user ADD COLUMN is_banned BOOLEAN DEFAULT FALSE` → `SyntaxError: syntax error at or near "user"`. So `is_banned`/`last_login_at` were never created and every ORM user query died with `UndefinedColumn: column user.is_banned does not exist` | `_migration_ddl()` now quotes identifiers on PostgreSQL (`ALTER TABLE "user" ADD COLUMN "is_banned" …`); failed migration steps log at ERROR | FIXED — unit-tested (`test_migration_ddl_quotes_postgres_reserved_words`) |
| Even a migration problem could keep the admin out | No fallback existed | `_fallback_user_lookup()` + login/`load_user` raw-SQL fallback on core columns; a truly unreachable DB still returns an actionable 503 | FIXED (`test_login_survives_a_missing_column_on_the_database`, `test_login_reports_database_failure_clearly`) |
| Schema drift was invisible | `/diag` did not check columns | `/diag` now returns `database.missing_columns` and `status: schema_incomplete` | FIXED (`test_diag_reports_missing_columns_and_ok_status`) |

Verified live during this audit (before the code fix): `/diag` HTTP 200 with
`database.connected = true`, `psycopg2_installed = true`,
`templates_found` all `true`; `/ping` → `{"msg":"pong","ok":true}`; `/login`
rendered the normal form. The remaining failure is the migration/code issue
above, which requires deploying v3.4.0.

## 2. Auto Video Poster — feature audit

| Requirement | Status |
|---|---|
| Telegram **Saved Messages** source (spec: `AUTO_VIDEO_FEATURE.md`) | **FIXED** — scans Saved Messages, stores only message IDs in the DB. Previously the UI offered it but the backend had no implementation. PASS |
| ZIP upload, video validation | PASS — zip-slip proof, unicode names kept, empty/zero-byte files skipped (`test_zip_extraction_keeps_unicode_and_skips_empty`) |
| Channel limit UI ⇄ backend identical | **FIXED** — one constant `MAX_AUTO_POST_CHANNELS` (default 50) served to the UI by `/bot/app_config`. Old backend hard-capped at 10. PASS at 1/10/11/50 (200) and 51 (400) |
| Post confirmed before counting as success | PASS — `posted_count` increases only after `send_file` returns; failed channels keep their queue position |
| Per-channel independence | PASS — one failing channel and one dead Saved Message do not stop the others |
| FloodWait handling | PASS (`test_flood_wait_is_honoured_then_retried`) |
| posted / failed / skipped counters | PASS — real counters stored in the DB and shown as badges |
| Pause / resume / stop / delete | PASS (`test_pause_resume_delete_lifecycle`) |
| Restart recovery, no duplicate posts | PASS (`test_recover_interrupted_tasks_resets_stuck_campaigns`); `posting` rows are re-armed, queue position advances only after a confirmed send |
| ZIP media lost on redeploy | Handled: campaign → `paused` + `needs_reupload` + a clear message telling the user to re-create it or switch source |
| Actual Telegram delivery | **NOT LIVE-TESTED** |

## 3. Messages section

Broadcast, Scheduled Messaging, Personal Chats, Bulk DMs, DM Campaigns, Campaign Scheduler, Group Messages, Group Link Campaign, Forwarder, Multi-account DM: parsing, dedupe, per-account validation, honest statuses, FloodWait, durable progress — all covered by tests (see §1 items 7, 9, 11, 12, 13). **Live Telegram sending: NOT LIVE-TESTED.**

## 4. Other tools (sidebar inventory)

Accounts/Login, Join Requests, Broadcast, Scheduled Tasks, Bulk Messaging, Auto Join, Search & Join, Group Links, Member Scraper, Add Members, Account Tools, Post Reactions, Leave Groups, Profile Manager, Forwarder, Story Viewer, AI Auto-Reply, Channel Scheduler, Scrape + DM, Username Checker, Admin Panel & User Management.

- **Verified by automated test / static contract:** every `/bot/*` endpoint called from `dashboard.html` and `admin.html` exists in the Flask url_map (`test_every_dashboard_endpoint_exists_in_backend`, `test_admin_panel_calls_only_existing_endpoints`); no duplicate element IDs; all routes survive an empty/None body; auth + ownership checks preserved.
- **NOT LIVE-TESTED:** every flow whose final step is a real Telegram API call (join, scrape, add members, react, view stories, AI auto-reply against a real provider, channel scheduler posting). These need a live account and were **not** executed.

## 5. Known limitations (not hidden)

1. **No live environment testing.** Telegram is replaced by an in-process fake in all tests.
2. **Uploaded ZIP videos cannot survive a Railway restart** (ephemeral disk). Use the Saved Messages source for restart-safe campaigns — the app now detects the loss and says so instead of failing silently.
3. **Legacy SQLite → PostgreSQL import** is implemented and idempotent but ships **disabled** (`MIGRATE_SQLITE=0`). Set `MIGRATE_SQLITE=auto` to import only when the target has no users. The real import path is **NOT LIVE-TESTED** (needs a Postgres target).
4. **One process only.** Background schedulers are guarded against double-start inside a process; do not run multiple Gunicorn workers (`python main.py` is the supported start command).
5. Telegram can revoke a session at any time; after that the account must be logged in again (documented in the Neon/Railway guides).
6. `telegram_client.py` is a standalone legacy helper, not used by `main.py`; it is included unchanged.
