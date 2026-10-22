# FEATURE_TEST_REPORT.md — MR ADS TG Automation

Work done **directly on the existing repository** (no demo project, no static mockup).

- **Repository:** `vvbc918-maker/probable-tribble` (private) — branch `main`
- **Account used:** `vvbc918-maker` (GitHub CLI device-flow authorization)
- **Commit for this update:** see the commit listed in the final chat message (pushed after this file)
- **Railway service:** `probable-tribble` (project *incredible-tranquility*, environment *production*)
- **Live app:** `https://probable-tribble-production-c360.up.railway.app`
- **Automated suite at the time of writing:** `143 passed` (`_evidence/pytest.txt`)

Status legend — **PASS** = executed by an automated test against real application
code (Telegram mocked). **NOT TESTED** = needs a live Telegram account, a second
Railway service or a real browser session, and was therefore **not** executed.
A page or button existing is never treated as PASS.

---

## 1. What was added in this update

### 1.1 One generic scheduling engine for every messaging tool
New model `ScheduledTask` (27 columns) + 10 new routes + a background worker
(`run_generic_scheduler`, registered through `start_job_once`, so it can never
start twice in one process):

| Route | Purpose |
|---|---|
| `GET /bot/scheduler/kinds` | the messaging tools that can be scheduled |
| `POST /bot/scheduler/create` | create a scheduled **or** immediate job (`send_now`) |
| `GET /bot/scheduler/list` | list + per-status counts |
| `POST /bot/scheduler/<id>/pause` `resume` `cancel` `run_now` | lifecycle |
| `PATCH /bot/scheduler/<id>` | edit message/destinations/time/repeat |
| `DELETE /bot/scheduler/<id>` | delete |
| `POST /bot/group_links/resolve` | validate + resolve group/channel links |

Supported task kinds: `broadcast`, `group_links`, `dm`, `dm_campaign`,
`personal`, `channel_post` — i.e. Group Broadcast, Scheduled Broadcasts,
Personal Chats, Bulk DMs, DM Campaigns, Group-Link messaging and Channel Posts.

Design decisions that answer the brief directly:
- **Durable:** every task, counter and error is stored in PostgreSQL
  (`scheduled_task` table) — created automatically by `db.create_all()`.
- **Never executed twice:** a due task is claimed with an atomic
  `UPDATE … WHERE status='pending'` guard (only one worker can move
  `pending → running`), plus a per-run `TaskRun` record.
- **Crash recovery:** a worker that dies leaves `claimed_at` stale; the loop
  re-arms it as `pending` **only if nothing was sent**, otherwise marks it
  `interrupted`.
- **Honest statuses:** `completed` (all confirmed) / `partial` / `failed`
  (zero confirmed sends is never "completed"), plus `paused`, `cancelled`.
- **Rate limits respected:** FloodWait is waited out and the destination is
  reported as `skipped` with the reason. No evasion, no account rotation.
- **Explicit multi-account strategy:** destinations are spread round-robin over
  the selected accounts and the strategy string is returned by the API
  (`execution_strategy`). No silent account switching.
- **Repeat semantics:** `repeat_count = 0` means ∞ (matches the existing UI
  label), `N` means N total runs — reusing `next_repeat_settings()`.

### 1.2 Frontend
- New **⏱ Scheduler** nav entry + full page (`#page-scheduler`) using the
  existing design system: kind selector, searchable multi-account selector with
  select-all/clear, destination box with **Validate & resolve links**, media
  type/URL, delay, `datetime-local` picker, repeat interval + count,
  **Schedule** / **Send now**, status filter, per-task counters, error list and
  Run now / Pause / Resume / Edit / Cancel / Delete actions.
- **⏱ "Schedule this"** button injected into the existing messaging pages
  (Broadcast, Bulk DMs, DM Campaigns, Group Links, Personal Chats) that copies
  the current form values into the scheduler — settings are preserved, not
  discarded by a redirect.
- Element ids of the injected block are prefixed `gs*` so they cannot collide
  with the page's own ids (an automated test guards this: duplicate-id check).

---

## 2. Feature-by-feature test report

| # | Feature | Status | Evidence |
|---|---|---|---|
| 1 | Scheduler validation for every kind (missing account, no destinations, unknown kind, no message/media) | **PASS** | `test_scheduler_validation_for_every_kind` |
| 2 | Date/time validation: past date, garbage date, repeat count without interval | **PASS** | `test_scheduler_rejects_bad_times_and_repeat_settings` |
| 3 | Create → list → pause → resume → edit → cancel → delete | **PASS** | `test_scheduler_full_lifecycle` |
| 4 | A due task runs **exactly once**, counters are honest (2/2 sent) | **PASS** | `test_scheduled_task_runs_exactly_once_with_honest_counts` |
| 5 | Claim guard: second execution attempt sends nothing again | **PASS** | same test (returns `False`, send count unchanged) |
| 6 | Per-destination failure → `partial`, reasons recorded | **PASS** | `test_scheduled_task_partial_failure_is_not_reported_as_success` |
| 7 | Recurring schedule re-arms (count 2 → 1 left, status `pending`, next run set) | **PASS** | `test_scheduled_task_repeat_rearms_without_duplicating` |
| 8 | Multi-account round-robin (4 destinations × 2 accounts → sent 4, total 8) | **PASS** | `test_multi_account_schedule_uses_round_robin` |
| 9 | Group-link validation/normalisation + resolution with unusable/invalid links | **PASS** | `test_group_link_resolution_reports_unusable_destinations`, `test_normalize_destination_list_dedupes_and_flags_invalid` |
| 10 | Destination de-duplication (`@alpha` = `t.me/alpha`) | **PASS** | `test_normalize_destination_list_dedupes_and_flags_invalid` |
| 11 | Schedules are durable rows in PostgreSQL | **PASS** (unit) | every scheduler test reads state back through `GET /bot/scheduler/list` after the DB write |
| 12 | Progress is durable across a lost in-memory cache (`TaskRun`) | **PASS** | `test_progress_survives_process_memory_loss` |
| 13 | No duplicate element ids in `dashboard.html` (incl. the new UI) | **PASS** | `test_dashboard_has_no_duplicate_element_ids` |
| 14 | Frontend endpoints all exist in the backend | **PASS** | `test_every_dashboard_endpoint_exists_in_backend` (dashboard now calls the scheduler routes) |
| 15 | Existing messaging flows still work after the change | **PASS** | the full pre-existing suite (broadcast, DMs, campaigns, forwarder, Auto Video Poster, schedules) still passes — 143/143 |
| 16 | Real Telegram delivery of a scheduled message | **NOT TESTED** | requires a live account; all send paths were executed against the in-process fake only |
| 17 | Recurring task over real wall-clock time (e.g. hourly) | **NOT TESTED** | needs a long-running live deployment |
| 18 | Railway restart with a pending schedule | **NOT TESTED** | needs a live redeploy; the claim guard + recovery logic is unit-covered (rows 4, 5, 12) |
| 19 | Group-link resolution against real private channels / invite links | **NOT TESTED** | needs live Telegram + membership |
| 20 | Browser-level UI interaction (clicking through the Scheduler page) | **NOT TESTED** | verification here is static (route contract, id uniqueness, HTML/JS wiring) |
| 21 | Mobile/tablet layout of the new page | **NOT TESTED** | uses the existing responsive design system; no device testing performed |

---

## 3. Root causes of the missing features (audit findings)

1. **Scheduling existed for only one tool.** `ScheduledBroadcast` +
   `run_scheduled_broadcasts()` served the Scheduled Broadcasts page only; the
   other messaging tools had no scheduling path at all. → replaced by the shared
   engine above (the old table/route remain untouched for compatibility).
2. **Group links were scrape-only.** There was no route to validate/resolve a
   link and send to it → added `POST /bot/group_links/resolve` + the
   `group_links` task kind.
3. **Inconsistent account/destination selection.** Each page had its own
   account `<select>` (and the Forwarder page's selector was previously
   unreachable due to duplicate ids — fixed earlier). → the scheduler uses one
   searchable multi-select component with status badges.
4. **Two of my own defects were caught by the new tests and fixed before
   shipping:** the injected UI reused existing ids (`schedAlert`, `schedDelay`,
   `schedRepeatCount`) and short "usernames" like `@a` were silently accepted as
   destinations. Both are now impossible.

## 4. Known limitations / remaining issues

1. **Nothing in rows 16–21 was live-tested.** They need a live Telegram account,
   a second Railway service or a real browser session.
2. **Telegram limits still apply.** A destination is only usable if the selected
   account can post there (membership, admin rights, invite-link joins). The
   resolver reports this instead of pretending it can force access.
3. **Media for scheduled posts** is a URL or a path that must exist on the
   service when the task fires; uploaded files on the Railway filesystem are
   ephemeral. Use URLs (or Telegram Saved Messages for the Auto Video Poster).
4. **Scheduler concurrency:** the claim guard protects against double runs
   inside one process and against stale claims. Running two Railway services
   against the same database with `BACKGROUND_JOBS_ENABLED=1` in both is not
   recommended.
5. **Editing a running task is intentionally blocked** (`cancel` then create a
   new one) so an in-flight send can never be mutated half-way.
6. `telegram_client.py` remains a standalone legacy helper, unused by `main.py`.

## 5. How this was verified (commands actually run)

```
python -m pytest tests/test_suite.py -q      ->  143 passed in 59.74s
python -c "import main; …"                   ->  IMPORT OK, 125 routes
                                               ScheduledTask columns: 27
```
Raw output: `_evidence/pytest.txt`. Failure modes are documented above rather
than hidden.
