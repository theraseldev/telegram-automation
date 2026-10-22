# Telegram Automation Dashboard

A full-featured Flask web dashboard for automating Telegram accounts using the Telethon library.

## Login
- URL: the project URL
- Default: `admin` / `admin123`

## Getting Started
1. Log in, go to **Accounts** tab
2. Enter your `API ID` and `API Hash` from [my.telegram.org](https://my.telegram.org)
3. Add Telegram accounts with phone numbers
4. All accounts persist permanently in SQLite

## Features

| Feature | Page | Notes |
|---------|------|-------|
| 📢 Group Broadcast | Broadcast | Groups where posting is not allowed (broadcast channels, restricted) are automatically disabled/greyed out and cannot be selected |
| 🕐 Scheduled Broadcast | Scheduled | Pick date/time, groups auto-execute in background |
| 💬 Personal Chats | Personal Chats | Loads all DM contacts, click to select, send messages |
| 📨 Bulk DMs | Bulk DMs | Send one message to many usernames |
| 🎯 DM Campaigns | DM Campaigns | Multi-message rotation, `{name}` personalization |
| 🔗 Auto Join | Auto Join | Paste invite links or usernames, bulk join |
| 🔍 Search & Join | Search & Join | Search by keyword, checkbox-select results to join |
| 🗂 Group Link Scraper | Group Links | Scrape all group/channel links you're in, export .txt |
| 👥 Member Scraper | Member Scraper | Scrape members from any group (supports active-only filter) |
| ➕ Add Members | Add Members | Add scraped members to another group |
| 🚨 Multi-Account Report | Report | All selected accounts report the same target; multiple report texts rotate per account |
| 🌐 Proxy Support | Accounts | SOCKS5/SOCKS4/HTTP proxy, global or per-account |

## Architecture

- **Backend**: Python 3.11 / Flask / SQLAlchemy
- **Database**: SQLite (`instance/telegram_automation.db`) — accounts never deleted unless manually removed
- **Auth**: Flask-Login, `remember=True`, 365-day session
- **Telegram**: Telethon (real API calls)
- **Background**: scheduled broadcast checker thread (30s interval), daily stats reset thread

## DB Models
- `User` — dashboard login accounts
- `TelegramAccount` — Telegram phone sessions (persisted forever)
- `BroadcastTask` — immediate broadcast tasks
- `ScheduledBroadcast` — future broadcasts
- `ProxyConfig` — SOCKS/HTTP proxy settings per account or global
- `ActivityLog` — all actions
- `Settings` — per-user API credentials

## Bug Fixes
- **Verification Code Expiry**: `verify_code()` now uses `StringSession(info['session_after_send'])` instead of empty `StringSession()`, so the same session that sent the code is reused for sign-in.

## Running
```bash
python3 main.py
```
Starts on port 5000.


## Auto Video Poster
- Upload multiple ZIP files at once.
- Supported video formats: MP4, MKV, MOV, WEBM, AVI, M4V.
- Select up to 10 channels; each queued video is posted to every selected channel.
- Random delay between videos: 5–15 minutes (configurable within that range).
- Caption supports `{filename}`, `{name}`, `{index}`, `{total}`.
- Campaigns support pause/resume/delete and progress tracking.
- The scheduler runs in the background.
- For Railway Free, uploaded media and SQLite are ephemeral across redeploys/restarts; use external storage/database if durable persistence is required.
- Default dashboard login for a fresh install: `admin` / `MRADS@TG2026!`.
- Set `ADMIN_PASSWORD` in Railway Variables to change the login password.
