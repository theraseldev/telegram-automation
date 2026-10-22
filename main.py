from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from datetime import datetime, timedelta, timezone
import asyncio
import json
import os
import re
import logging
import threading
import time
import uuid
import secrets
import string
import zipfile
import shutil
import random
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge

try:
    import socks
    SOCKS_AVAILABLE = True
except ImportError:
    SOCKS_AVAILABLE = False

try:
    import psycopg2  # noqa: F401  (PostgreSQL / Neon driver used by DATABASE_URL)
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False

from telethon import TelegramClient
from telethon.sessions import StringSession
import smtplib
from email.mime.text import MIMEText
try:
    import requests as http_requests
    HTTP_REQUESTS_OK = True
except ImportError:
    HTTP_REQUESTS_OK = False

from telethon.errors import (
    FloodWaitError, PhoneCodeInvalidError, SessionPasswordNeededError,
    PhoneCodeExpiredError, UserAlreadyParticipantError, ChatAdminRequiredError,
    ChannelPrivateError, UsernameInvalidError,
    UserDeactivatedError, UserDeactivatedBanError, AuthKeyUnregisteredError,
    AuthKeyDuplicatedError
)
from telethon.tl import functions, types
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import ReportRequest

app = Flask(__name__)
# Trust the platform HTTPS proxy (Railway / Replit) so Secure cookies work correctly
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLite fallback location (used only when DATABASE_URL is not configured).
_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'telegram_automation.db')

# ─── RUNTIME CONFIG (single source of truth for UI + backend limits) ──────────

def _env_int(name, default, minimum=None, maximum=None):
    """Read an int from the environment with a safe fallback (never raises)."""
    try:
        value = int(str(os.environ.get(name, '')).strip() or default)
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


# ─── LIMITS ──────────────────────────────────────────────────────────────────
# Every ceiling is optional. Defaults are deliberately enormous so the app does
# NOT cap your work — set the matching environment variable only if YOU want a
# limit. `0` means "no limit" wherever it makes sense.
UNLIMITED = 100_000_000


def _limit_env(name, default):
    """Read an optional ceiling from the environment.

    `0` (or leaving the variable unset) means "no limit". Without this mapping a
    value of 0 would have clamped every request to 0 instead of freeing it.
    """
    value = _env_int(name, default, 0, UNLIMITED)
    return UNLIMITED if value == 0 else value


# Auto Video Poster channel ceiling — used by BOTH the dashboard UI (through
# /bot/app_config) and the backend validation, so the two can never disagree.
MAX_AUTO_POST_CHANNELS = _limit_env('MAX_AUTO_POST_CHANNELS', 100000)
AUTO_POST_DELAY_MIN = 1
AUTO_POST_DELAY_MAX = _limit_env('MAX_DELAY_MINUTES', 10080)          # 7 days
MAX_UPLOAD_MB = _env_int('MAX_UPLOAD_MB', 4096, 0, 1048576)           # 0 = no limit
MAX_DELAY_SECONDS = _limit_env('MAX_DELAY_SECONDS', 604800)           # 7 days
MAX_REPEAT_INTERVAL_HOURS = _limit_env('MAX_REPEAT_INTERVAL_HOURS', 876000)   # 100 years
MAX_REPEAT_COUNT = _limit_env('MAX_REPEAT_COUNT', 1000000)
MAX_FORWARD_MESSAGES = _limit_env('MAX_FORWARD_MESSAGES', 100000)     # 0 = all messages
MAX_HISTORY_LIMIT = _limit_env('MAX_HISTORY_LIMIT', 100000)
MAX_LIST_ROWS = _limit_env('MAX_LIST_ROWS', 100000)
MAX_BULK_COUNT = _limit_env('MAX_BULK_COUNT', 1000000)
MAX_SCRAPE_LIMIT = _limit_env('MAX_SCRAPE_LIMIT', 1000000)
MAX_JOIN_DAILY_LIMIT = _limit_env('MAX_JOIN_DAILY_LIMIT', 1000000)
MAX_SCHEDULED_BATCH = _limit_env('MAX_SCHEDULED_BATCH', 5000)
AUTO_POST_MAX_CONCURRENT = _limit_env('AUTO_POST_MAX_CONCURRENT', 25)
SAVED_MESSAGES_SCAN_LIMIT = _limit_env('SAVED_MESSAGES_SCAN_LIMIT', 1000000)
LOG_ROWS_LIMIT = _limit_env('LOG_ROWS_LIMIT', 100000)
# A FloodWait can legitimately require a long wait; do not truncate it.
MAX_FLOOD_WAIT_SECONDS = _limit_env('MAX_FLOOD_WAIT_SECONDS', 86400)
ALLOW_REGISTRATION = str(os.environ.get('ALLOW_REGISTRATION', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
BACKGROUND_JOBS_ENABLED = str(os.environ.get('BACKGROUND_JOBS_ENABLED', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
STALE_TASK_MINUTES = _env_int('STALE_TASK_MINUTES', 30, 5, 1440)


# ─── DATABASE (PostgreSQL/Neon persistent storage, SQLite local fallback) ─────

def _normalize_db_url(raw_url):
    """Normalize a database URL for SQLAlchemy + psycopg2.

    Railway/Neon/Heroku hand out ``postgres://`` or ``postgresql://`` URLs which
    SQLAlchemy 2.x rejects. Missing TLS is made explicit because Neon requires it.
    Unknown schemes are returned unchanged so DATABASE_URL may point elsewhere.
    """
    url = str(raw_url or '').strip().strip('"').strip("'")
    if not url:
        return ''
    for prefix in ('postgres://', 'postgresql://'):
        if url.startswith(prefix):
            url = 'postgresql+psycopg2://' + url[len(prefix):]
            break
    if url.startswith('postgresql+psycopg://'):
        url = 'postgresql+psycopg2://' + url[len('postgresql+psycopg://'):]
    # The Neon console likes to add channel_binding=require. Older libpq/psycopg2
    # builds reject that keyword, which made EVERY database call fail (and, via
    # the session loader, every page including /login). TLS is still enforced by
    # sslmode=require below, so dropping it is safe.
    if 'channel_binding=' in url:
        base, _, query = url.partition('?')
        kept = [part for part in query.split('&')
                if part and not part.startswith('channel_binding=')]
        url = base + (('?' + '&'.join(kept)) if kept else '')
    if url.startswith('postgresql+psycopg2://') and 'sslmode=' not in url:
        url += ('&' if '?' in url else '?') + 'sslmode=require'
    return url


def _resolve_database_uri():
    """Return (uri, source_label, is_postgres). DATABASE_URL wins; SQLite is the fallback."""
    for env_name in ('DATABASE_URL', 'POSTGRES_URL', 'POSTGRESQL_URL', 'NEON_DATABASE_URL'):
        raw = os.environ.get(env_name, '')
        if str(raw or '').strip():
            uri = _normalize_db_url(raw)
            is_pg = uri.startswith('postgresql')
            if is_pg and not PSYCOPG_AVAILABLE:
                logger.error(
                    "%s points at PostgreSQL but the psycopg2 driver is not installed. "
                    "Install it with: pip install psycopg2-binary", env_name,
                )
            return uri, env_name, is_pg
    return f'sqlite:///{_db_path}', 'sqlite-fallback', False


DB_URI, DB_SOURCE, DB_IS_POSTGRES = _resolve_database_uri()

app.config['SECRET_KEY'] = (
    os.environ.get('SECRET_KEY')
    or os.environ.get('SESSION_SECRET')
    or secrets.token_urlsafe(48)  # replaced by a persisted key in init_app_schema()
)
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=365)
# SameSite=None + Secure keeps cookies working in embedded/iframe hosting
app.config['SESSION_COOKIE_SAMESITE'] = 'None'
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['REMEMBER_COOKIE_DURATION'] = timedelta(days=365)
app.config['REMEMBER_COOKIE_SAMESITE'] = 'None'
app.config['REMEMBER_COOKIE_SECURE'] = True
app.config['MAX_CONTENT_LENGTH'] = (MAX_UPLOAD_MB * 1024 * 1024) if MAX_UPLOAD_MB > 0 else None  # 0 = no limit

if DB_IS_POSTGRES:
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,   # drop dead Neon connections instead of erroring
        'pool_recycle': 280,     # Neon closes idle connections
        'pool_size': 5,
        'max_overflow': 10,
    }

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@app.context_processor
def inject_runtime_flags():
    """Expose runtime flags to templates (e.g. whether sign-up is enabled)."""
    return {
        'registration_enabled': ALLOW_REGISTRATION,
        'max_auto_post_channels': MAX_AUTO_POST_CHANNELS,
    }


@login_manager.unauthorized_handler
def unauthorized():
    """Return JSON error for API routes, redirect for page routes."""
    if request.path.startswith('/bot/'):
        return jsonify({'success': False, 'error': 'Session expired. Please refresh the page and log in again.', 'login_required': True}), 401
    return redirect(url_for('login'))

@app.after_request
def add_no_cache_headers(response):
    """Prevent browser from caching API responses."""
    if request.path.startswith('/bot/') or request.path == '/dashboard':
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response


@app.errorhandler(RequestEntityTooLarge)
def handle_request_too_large(error):
    """Uploads above MAX_UPLOAD_MB answer with a clear JSON message, not an HTML error page."""
    return jsonify({
        'success': False,
        'error': f'Upload is larger than the {MAX_UPLOAD_MB} MB limit. Split the file or raise MAX_UPLOAD_MB.',
    }), 413


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    """Return valid JSON for API failures and a readable page for browser routes."""
    from werkzeug.exceptions import HTTPException
    if isinstance(error, HTTPException):
        if request.path.startswith('/bot/'):
            return jsonify({'success': False, 'error': error.description}), error.code or 500
        return error
    logger.exception('Unhandled error on %s: %s', request.path, error)
    if request.path.startswith('/bot/'):
        return jsonify({
            'success': False,
            'error': 'Internal server error. Check the server logs for the detailed reason.',
        }), 500
    # A plain "Internal server error" text told the operator nothing. This page
    # names the likely cause and links to /diag, which reports the real state.
    return _debug_hint_page(type(error).__name__, str(error)), 500


def _debug_hint_page(title, detail):
    """Small HTML page shown when a browser route fails (safe: no secrets)."""
    templates_dir = os.path.join(app.root_path, app.template_folder or 'templates')
    missing = [name for name in ('login.html', 'dashboard.html', 'admin.html')
               if not os.path.exists(os.path.join(templates_dir, name))]
    hints = [
        f'<li>Error type: <code>{html_escape(title)}</code></li>',
        f'<li>Detail: <code>{html_escape(detail)[:400]}</code></li>',
        f'<li>Templates directory: <code>{html_escape(templates_dir)}</code></li>',
        f'<li>Missing template files: <code>{html_escape(", ".join(missing) or "none")}</code></li>',
        f'<li>Database: <code>{html_escape("postgresql" if DB_IS_POSTGRES else "sqlite")} via {html_escape(DB_SOURCE)}</code></li>',
        f'<li>Database init error: <code>{html_escape(DB_INIT_ERROR or "none")}</code></li>',
        '<li>SECRET_KEY taken from the environment: <code>%s</code></li>' % (
            'yes' if (os.environ.get('SECRET_KEY') or os.environ.get('SESSION_SECRET')) else 'no'),
    ]
    return f'''<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Startup problem — MR ADS TG Automation</title>
<style>body{{font-family:system-ui,Segoe UI,sans-serif;background:#070b14;color:#e8eefc;padding:40px;line-height:1.6}}
code{{background:#131c2e;padding:2px 6px;border-radius:6px}}a{{color:#22d3ee}}
.box{{max-width:760px;margin:0 auto;background:rgba(20,28,45,.75);border:1px solid rgba(120,150,200,.18);
border-radius:14px;padding:26px}}h1{{font-size:20px;margin-bottom:14px}}</style></head><body><div class="box">
<h1>⚠️ A page could not be rendered</h1>
<p>Usually this means a template file is missing from the deployment, or the database is misconfigured.</p>
<ul>{''.join(hints)}</ul>
<p>Full machine-readable state: <a href="/diag">/diag</a> &nbsp;·&nbsp; <a href="/login">try login again</a></p>
<p>Railway shows the complete Python traceback under <strong>Deployments → View Logs</strong>.</p>
</div></body></html>'''


def html_escape(value):
    """Minimal HTML escaping for the diagnostic page."""
    return (str(value or '')
            .replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            .replace('"', '&quot;'))


@app.route('/diag')
def diagnostics():
    """Unauthenticated, secret-free readiness report (safe to open in a browser).

    Use this immediately after a deploy: it tells you whether templates shipped,
    whether the database is reachable and which variables are still placeholders.
    """
    templates_dir = os.path.join(app.root_path, app.template_folder or 'templates')
    secret = os.environ.get('SECRET_KEY') or os.environ.get('SESSION_SECRET') or ''
    info = {
        'app': 'MR ADS TG Automation',
        'status': 'ok',
        'templates_found': {
            name: os.path.exists(os.path.join(templates_dir, name))
            for name in ('login.html', 'dashboard.html', 'admin.html')
        },
            'templates_dir': templates_dir,
            'auto_video_media': {
                'root': AUTO_VIDEO_ROOT,
                'writable': os.access(AUTO_VIDEO_ROOT, os.W_OK)
                            if os.path.isdir(AUTO_VIDEO_ROOT) else False,
                # True when the root lives on a mounted volume instead of the
                # ephemeral container filesystem, i.e. uploads survive a redeploy.
                'persistent_hint': bool(os.environ.get('AUTO_POST_MEDIA_DIR'))
                                    or AUTO_VIDEO_ROOT.startswith(os.path.join(os.sep, 'data') + os.sep),
            },
        'database': {
            'backend': 'postgresql' if DB_IS_POSTGRES else 'sqlite',
            'source_variable': DB_SOURCE,
            'psycopg2_installed': PSYCOPG_AVAILABLE,
            'connected': False,
            'init_error': DB_INIT_ERROR,
        },
        'environment': {
            'SECRET_KEY_set': bool(secret),
            'SECRET_KEY_is_placeholder': secret in ('replace-with-a-long-random-string', 'change-this-to-a-long-random-secret'),
            'ADMIN_PASSWORD_set': bool(os.environ.get('ADMIN_PASSWORD')),
            'DATABASE_URL_set': bool(os.environ.get('DATABASE_URL')),
            'background_jobs': BACKGROUND_JOBS_ENABLED,
            'allow_registration': ALLOW_REGISTRATION,
        },
        'limits': {
            'max_auto_post_channels': MAX_AUTO_POST_CHANNELS,
            'max_upload_mb': MAX_UPLOAD_MB,
            'max_delay_seconds': MAX_DELAY_SECONDS,
            'max_forward_messages': MAX_FORWARD_MESSAGES,
        },
    }
    try:
        from sqlalchemy import text as sa_text
        db.session.execute(sa_text('SELECT 1'))
        info['database']['connected'] = True
    except Exception as e:
        info['database']['connected'] = False
        info['database']['error'] = str(e)[:300]
        info['status'] = 'database_unreachable'
    # Report schema drift explicitly: a column that the migration could not add
    # is the reason ORM queries (login!) fail even while the DB is reachable.
    try:
        from sqlalchemy import inspect as sa_inspect
        inspector = sa_inspect(db.engine)
        existing_tables = set(inspector.get_table_names())
        missing = []
        for table, column, _sqlite_type, _pg_type in SCHEMA_COLUMNS:
            if table not in existing_tables:
                missing.append(f'{table} (table missing)')
                continue
            if column not in {c['name'] for c in inspector.get_columns(table)}:
                missing.append(f'{table}.{column}')
        info['database']['missing_columns'] = missing
        if missing and info['status'] == 'ok':
            info['status'] = 'schema_incomplete'
    except Exception as e:
        info['database']['missing_columns'] = ['check failed: ' + str(e)[:120]]
    if not all(info['templates_found'].values()):
        info['status'] = 'templates_missing'
    if info['environment']['SECRET_KEY_is_placeholder']:
        info['status'] = 'placeholder_secret_key'
    return jsonify(info)

# ─── SAFE INPUT HELPERS (no more "int() argument ... not 'NoneType'") ─────────

def safe_int(value, default=0, minimum=None, maximum=None):
    """Convert any optional form/JSON/query value to int without ever raising.

    ``None``, ``''``, ``'abc'`` and objects all fall back to ``default`` so the
    caller can decide whether an acceptable default exists and validate.
    """
    if value is None or isinstance(value, bool):
        result = default
    else:
        try:
            text_value = str(value).strip()
            if text_value == '':
                result = default
            elif '.' in text_value or 'e' in text_value.lower():
                result = int(float(text_value))
            else:
                result = int(text_value)
        except (TypeError, ValueError):
            result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def safe_float(value, default=0.0):
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def clean_str(value, default=''):
    """Trim any optional value into a string (never returns None)."""
    if value is None:
        return default
    try:
        return str(value).strip()
    except Exception:
        return default


def parse_datetime_utc(value):
    """Parse ISO-8601 input and normalise it to naive UTC. Returns None if invalid."""
    raw = clean_str(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00').replace('z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def clean_str_list(raw, split_lines_only=False):
    """Turn a list, comma string or newline string into a clean, de-duplicated list."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        items = [clean_str(x) for x in raw]
    else:
        text_value = clean_str(raw)
        if not text_value:
            return []
        parts = text_value.splitlines() if split_lines_only else re.split(r'[\n,]+', text_value)
        items = [clean_str(p) for p in parts]
    return dedupe_preserve([i for i in items if i])


def dedupe_preserve(items, key=None):
    """Remove duplicates while preserving the original order."""
    seen = set()
    result = []
    for item in items or []:
        marker = key(item) if key else item
        if marker in seen:
            continue
        seen.add(marker)
        result.append(item)
    return result


def redact(value, keep=3):
    """Mask a secret before it reaches a log line."""
    text_value = str(value or '')
    if len(text_value) <= keep:
        return '*' * len(text_value)
    return text_value[:keep] + '...' + '*' * max(4, len(text_value) // 4)


def parse_channel_target(raw):
    """Normalise a channel/group input for Telethon.

    Returns None for empty input, '' for malformed input (caller should reject),
    otherwise {'kind': 'username'|'invite'|'private_post'|'id', 'value': str}.
    """
    text_value = clean_str(raw)
    if not text_value:
        return None
    text_value = text_value.replace('https://', '').replace('http://', '').strip().strip('/')
    for prefix in ('t.me/', 'telegram.me/', 'www.t.me/'):
        if text_value.lower().startswith(prefix):
            text_value = text_value[len(prefix):]
    text_value = text_value.strip()
    if text_value.startswith('+'):
        return {'kind': 'invite', 'value': text_value, 'raw': raw}
    if text_value.lower().startswith('joinchat/'):
        return {'kind': 'invite', 'value': '+' + text_value[len('joinchat/'):], 'raw': raw}
    # Private post links appear as t.me/c/<internal_id>/<msg_id>; after the
    # t.me/ prefix is stripped this becomes c/<internal_id>/<msg_id>.
    private_part = None
    if text_value.startswith('c/'):
        private_part = text_value[2:]
    elif '/c/' in text_value:
        private_part = text_value.split('/c/')[-1]
    if private_part is not None:
        parts = [p for p in private_part.split('/') if p]
        if parts and parts[0].isdigit():
            return {'kind': 'private_post', 'value': parts[0], 'raw': raw}
        return ''
    if '/' in text_value:
        text_value = text_value.split('/')[0]
    text_value = text_value.lstrip('@').strip()
    if not text_value:
        return ''
    if re.fullmatch(r'-?\d+', text_value):
        return {'kind': 'id', 'value': text_value, 'raw': raw}
    if not re.fullmatch(r'[A-Za-z0-9_]{4,64}', text_value):
        return ''
    return {'kind': 'username', 'value': text_value, 'raw': raw}


def friendly_telegram_error(exc, target=''):
    """Turn a Telethon/network exception into a short, safe, actionable message."""
    prefix = f'{target}: ' if target else ''
    try:
        if isinstance(exc, FloodWaitError):
            return f'{prefix}Telegram rate limit — retry after {safe_int(getattr(exc, "seconds", 0))}s'
        if isinstance(exc, ChatAdminRequiredError):
            return f'{prefix}this account is not an admin of that chat'
        if isinstance(exc, ChannelPrivateError):
            return f'{prefix}the channel is private or this account has not joined it'
        if isinstance(exc, (UserDeactivatedError, UserDeactivatedBanError)):
            return f'{prefix}the target account is deactivated or banned'
        if isinstance(exc, (AuthKeyUnregisteredError, AuthKeyDuplicatedError)):
            return f'{prefix}Telegram session is no longer valid — re-login required'
        if isinstance(exc, UserAlreadyParticipantError):
            return f'{prefix}already a participant'
        if isinstance(exc, UsernameInvalidError):
            return f'{prefix}username is invalid or does not exist'
    except Exception:
        pass
    return f'{prefix}{type(exc).__name__}: {exc}'


# ─── RUNTIME STATE / CONCURRENCY GUARDS ──────────────────────────────────────

pending_logins = {}
task_progress = {}          # in-memory cache; TaskRun (DB) is the durable record
_account_locks = {}         # account_id -> Lock, serialises campaigns per account
_guards_lock = threading.Lock()
_STARTED_JOBS = set()       # prevents double-starting background threads


def get_account_lock(account_id):
    """Return the process-wide lock that serialises work on one Telegram account."""
    with _guards_lock:
        lock = _account_locks.get(account_id)
        if lock is None:
            lock = threading.Lock()
            _account_locks[account_id] = lock
        return lock


def account_busy(account_id):
    """True when another campaign is already using this account."""
    return get_account_lock(account_id).locked()


def start_job_once(name, target):
    """Start a daemon background thread exactly once per process."""
    if not BACKGROUND_JOBS_ENABLED:
        logger.info('Background job %s skipped (BACKGROUND_JOBS_ENABLED=0)', name)
        return False
    with _guards_lock:
        if name in _STARTED_JOBS:
            logger.info('Background job %s already started in this process; skipping duplicate', name)
            return False
        _STARTED_JOBS.add(name)
    threading.Thread(target=target, name=name, daemon=True).start()
    return True


# ─── MODELS ──────────────────────────────────────────────────────────────────

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # is_banned existed only as a raw ALTER TABLE column before, so the ORM never
    # knew about it and bans were never persisted. It is a real column now.
    is_banned = db.Column(db.Boolean, default=False)
    last_login_at = db.Column(db.DateTime, nullable=True)
    accounts = db.relationship('TelegramAccount', backref='user', lazy=True, cascade='all, delete-orphan')
    logs = db.relationship('ActivityLog', backref='user', lazy=True, cascade='all, delete-orphan')
    tasks = db.relationship('BroadcastTask', backref='user', lazy=True, cascade='all, delete-orphan')


class TelegramAccount(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    session_string = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_used = db.Column(db.DateTime, default=datetime.utcnow)
    messages_sent_today = db.Column(db.Integer, default=0)
    reports_sent_today = db.Column(db.Integer, default=0)
    members_scraped = db.Column(db.Integer, default=0)
    members_added = db.Column(db.Integer, default=0)
    total_messages = db.Column(db.Integer, default=0)
    total_reports = db.Column(db.Integer, default=0)


class ActivityLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    action = db.Column(db.String(500), nullable=False)
    status = db.Column(db.String(50), default='success')
    details = db.Column(db.Text)
    account_id = db.Column(db.Integer, nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)


class BroadcastTask(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    message_type = db.Column(db.String(20))
    content = db.Column(db.Text)
    groups = db.Column(db.Text)
    delay = db.Column(db.Integer, default=2)
    status = db.Column(db.String(50), default='pending')
    sent_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)
    # ── Repeating rounds ─────────────────────────────────────────────────────
    # These live in the database, not in the thread that started the run, so a
    # redeploy/restart can resume a campaign that is waiting between rounds.
    caption = db.Column(db.Text)
    repeat = db.Column(db.Integer, default=1)        # total rounds; 0 = forever
    rounds_done = db.Column(db.Integer, default=0)   # rounds already sent
    next_round_at = db.Column(db.DateTime, nullable=True)  # when round N+1 is due
    task_key = db.Column(db.String(64))


class ScheduledBroadcast(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    message_type = db.Column(db.String(20), default='text')
    content = db.Column(db.Text, nullable=False)
    caption = db.Column(db.Text, nullable=True)
    groups = db.Column(db.Text, nullable=False)
    delay = db.Column(db.Integer, default=3)
    scheduled_at = db.Column(db.DateTime, nullable=False)
    status = db.Column(db.String(50), default='pending')
    sent_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)
    repeat_interval = db.Column(db.Integer, default=0)  # hours, 0 = no repeat
    repeat_count = db.Column(db.Integer, default=0)     # remaining repeats, 0 = unlimited


class ProxyConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id = db.Column(db.Integer, nullable=True)
    proxy_type = db.Column(db.String(10), default='socks5')
    host = db.Column(db.String(200), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    username = db.Column(db.String(100), nullable=True)
    password = db.Column(db.String(100), nullable=True)
    is_active = db.Column(db.Boolean, default=True)


class Settings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    api_id = db.Column(db.String(30), nullable=True)
    api_hash = db.Column(db.String(100), nullable=True)


class AlertSettings(db.Model):
    """Notification settings — Telegram bot alert + email alert."""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    # Telegram bot notification
    tg_bot_token = db.Column(db.String(200), nullable=True)   # bot token
    tg_chat_id   = db.Column(db.String(100), nullable=True)   # chat/group/channel id
    # Email notification
    email_to     = db.Column(db.String(200), nullable=True)   # recipient email
    smtp_host    = db.Column(db.String(200), nullable=True, default='smtp.gmail.com')
    smtp_port    = db.Column(db.Integer, nullable=True, default=587)
    smtp_user    = db.Column(db.String(200), nullable=True)
    smtp_pass    = db.Column(db.String(200), nullable=True)
    # Flags
    tg_enabled   = db.Column(db.Boolean, default=False)
    email_enabled= db.Column(db.Boolean, default=False)
    updated_at   = db.Column(db.DateTime, default=datetime.utcnow)


class AccountHealth(db.Model):
    """Latest health/restriction status per Telegram account."""
    id               = db.Column(db.Integer, primary_key=True)
    account_id       = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False, unique=True)
    user_id          = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    # ok | limited | banned | deactivated | auth_error | unknown
    status           = db.Column(db.String(30), default='unknown')
    detail           = db.Column(db.String(500), nullable=True)
    checked_at       = db.Column(db.DateTime, default=datetime.utcnow)
    alerted          = db.Column(db.Boolean, default=False)
    tg_name          = db.Column(db.String(200), nullable=True)   # Telegram display name
    tg_username      = db.Column(db.String(100), nullable=True)   # Telegram @username
    spambot_released = db.Column(db.Boolean, default=False)       # auto-release attempted?
    spambot_result   = db.Column(db.String(500), nullable=True)   # SpamBot response


class Subscription(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    plan = db.Column(db.String(20), default='free')  # free, basic, pro, unlimited
    expires_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)


class LicenseKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    plan = db.Column(db.String(20), default='basic')
    duration_days = db.Column(db.Integer, default=30)
    max_uses = db.Column(db.Integer, default=1)
    use_count = db.Column(db.Integer, default=0)
    used_by_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    used_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    notes = db.Column(db.String(300), nullable=True)


class AutoReplyBot(db.Model):
    """AI-powered auto DM reply bot config per account."""
    id           = db.Column(db.Integer, primary_key=True)
    user_id      = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id   = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False, unique=True)
    enabled      = db.Column(db.Boolean, default=False)
    provider     = db.Column(db.String(20), default='openai')   # 'openai' | 'gemini'
    openai_key   = db.Column(db.String(300), nullable=True)
    gemini_key   = db.Column(db.String(300), nullable=True)
    system_prompt= db.Column(db.Text, nullable=True)
    model        = db.Column(db.String(80), default='gpt-4o-mini')
    last_msg_id      = db.Column(db.Integer, default=0)
    service_keywords = db.Column(db.Text, nullable=True)   # comma-separated services
    account_context  = db.Column(db.Text, nullable=True)   # AI-generated account profile from own channels
    created_at       = db.Column(db.DateTime, default=datetime.utcnow)


class GlobalSetting(db.Model):
    """Admin-controlled global key-value settings."""
    __tablename__ = 'global_setting'
    key   = db.Column(db.String(100), primary_key=True)
    value = db.Column(db.Text, nullable=True)

    @staticmethod
    def get(key, default=None):
        try:
            obj = GlobalSetting.query.get(key)
            return obj.value if obj else default
        except Exception:
            return default

    @staticmethod
    def set_val(key, value):
        obj = GlobalSetting.query.get(key)
        if obj:
            obj.value = value
        else:
            db.session.add(GlobalSetting(key=key, value=value))


class ChannelScheduledPost(db.Model):
    """Scheduled post to own channel/group."""
    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id      = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    channel         = db.Column(db.String(200), nullable=False)
    content         = db.Column(db.Text, nullable=False)
    message_type    = db.Column(db.String(20), default='text')
    scheduled_at    = db.Column(db.DateTime, nullable=False)
    status          = db.Column(db.String(30), default='pending')
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    sent_at         = db.Column(db.DateTime, nullable=True)
    repeat_interval = db.Column(db.Integer, default=0)   # hours, 0 = no repeat
    media_path      = db.Column(db.Text, nullable=True)  # device-uploaded photo/video


class AutoVideoPostCampaign(db.Model):
    """Queue of videos (ZIP upload or Telegram Saved Messages) posted to channels.

    All scheduling state lives in the database (not on the local filesystem) so a
    Railway restart or a brand-new service pointed at the same Neon database
    resumes instead of losing or re-sending videos.
    """
    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id      = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    channels        = db.Column(db.Text, nullable=False)  # JSON list of channel usernames/links
    media_dir       = db.Column(db.String(500), nullable=False)
    caption         = db.Column(db.Text, nullable=True)
    # JSON list of caption templates. ONE is chosen at random per round and used
    # for every channel in that round (so a single video never shows two different
    # captions across channels). The single `caption` column above stays as the
    # fallback so existing campaigns keep working unchanged.
    captions        = db.Column(db.Text, nullable=True)
    min_delay       = db.Column(db.Integer, default=5)
    max_delay       = db.Column(db.Integer, default=15)
    current_index   = db.Column(db.Integer, default=0)
    total_videos    = db.Column(db.Integer, default=0)
    status          = db.Column(db.String(30), default='running')  # running/paused/posting/completed/failed
    next_run_at     = db.Column(db.DateTime, nullable=True)
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    last_post_at    = db.Column(db.DateTime, nullable=True)
    last_error      = db.Column(db.Text, nullable=True)
    # ── durable source + queue state (added for restart-safe campaigns) ──
    source_type     = db.Column(db.String(20), default='zip')   # 'zip' | 'saved_messages'
    source_ids      = db.Column(db.Text, nullable=True)         # JSON list of Saved Message IDs
    queue_state     = db.Column(db.Text, nullable=True)         # JSON per-channel shuffled queues
    posted_count    = db.Column(db.Integer, default=0)          # confirmed Telegram posts (all rounds)
    failed_count    = db.Column(db.Integer, default=0)
    skipped_count   = db.Column(db.Integer, default=0)
    rounds_completed= db.Column(db.Integer, default=0)
    stop_requested  = db.Column(db.Boolean, default=False)
    needs_reupload  = db.Column(db.Boolean, default=False)      # ZIP media lost (ephemeral disk)
    last_round_summary = db.Column(db.Text, nullable=True)
    worker_heartbeat = db.Column(db.DateTime, nullable=True)
    # Telegram-side copy of every posted video ({filename: {c: chat, m: msg_id}}).
    # This is what makes a ZIP campaign survive the ephemeral Railway filesystem.
    vault_state     = db.Column(db.Text, nullable=True)


class TaskRun(db.Model):
    """Durable progress record for background campaigns (survives restarts).

    In-memory ``task_progress`` remains a fast cache, but every campaign also
    writes here so status can be reported after a Railway restart/redeploy.
    """
    id          = db.Column(db.Integer, primary_key=True)
    task_key    = db.Column(db.String(64), unique=True, nullable=False, index=True)
    user_id     = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    kind        = db.Column(db.String(40), default='task')
    status      = db.Column(db.String(30), default='running')   # running/completed/failed/partial/stopped
    sent        = db.Column(db.Integer, default=0)
    failed      = db.Column(db.Integer, default=0)
    skipped     = db.Column(db.Integer, default=0)
    total       = db.Column(db.Integer, default=0)
    errors      = db.Column(db.Text, nullable=True)
    created_at  = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at  = db.Column(db.DateTime, default=datetime.utcnow)


class SavedTarget(db.Model):
    """Persistent saved channels/groups per user (stored in PostgreSQL/Neon)."""
    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    kind        = db.Column(db.String(20), default='channel')   # 'channel' | 'group'
    identifier  = db.Column(db.String(200), nullable=False)
    title       = db.Column(db.String(200), nullable=True)
    notes       = db.Column(db.String(300), nullable=True)
    created_at  = db.Column(db.DateTime, default=datetime.utcnow)


class ScrapeDMTask(db.Model):
    """Scheduled: scrape group members then DM them."""
    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id  = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    group_link  = db.Column(db.String(200), nullable=False)
    scrape_limit= db.Column(db.Integer, default=100)
    active_only = db.Column(db.Boolean, default=True)
    dm_message  = db.Column(db.Text, nullable=False)
    dm_delay    = db.Column(db.Integer, default=5)
    scheduled_at= db.Column(db.DateTime, nullable=False)
    status      = db.Column(db.String(30), default='pending')
    scraped     = db.Column(db.Integer, default=0)
    sent        = db.Column(db.Integer, default=0)
    created_at  = db.Column(db.DateTime, default=datetime.utcnow)


class MemberAlertBot(db.Model):
    """Auto-DM new members who join a monitored group."""
    id             = db.Column(db.Integer, primary_key=True)
    user_id        = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    account_id     = db.Column(db.Integer, db.ForeignKey('telegram_account.id'), nullable=False)
    group          = db.Column(db.String(200), nullable=False)
    welcome_msg    = db.Column(db.Text, nullable=False)
    enabled        = db.Column(db.Boolean, default=True)
    delay          = db.Column(db.Integer, default=3)
    last_member_id = db.Column(db.Integer, default=0)
    created_at     = db.Column(db.DateTime, default=datetime.utcnow)


# ─── PLAN LIMITS ──────────────────────────────────────────────────────────────

PLAN_LIMITS = {
    'free':      {'accounts': 9999, 'msgs_per_day': 99999, 'label': 'Free',      'color': '#8b949e', 'price': '$0'},
    'basic':     {'accounts': 9999, 'msgs_per_day': 99999, 'label': 'Basic',     'color': '#3fb950', 'price': '$9.99/mo'},
    'pro':       {'accounts': 9999, 'msgs_per_day': 99999, 'label': 'Pro',       'color': '#00d4ff', 'price': '$24.99/mo'},
    'unlimited': {'accounts': 9999, 'msgs_per_day': 99999, 'label': 'Unlimited', 'color': '#f0c040', 'price': '$49.99/mo'},
}

PLAN_FEATURES = {
    'free':      ['broadcast', 'personal'],
    'basic':     ['broadcast', 'personal', 'dms', 'schedule', 'autojoin', 'scraper', 'actools'],
    'pro':       ['all'],
    'unlimited': ['all'],
}

ADMIN_USERS = {'admin'}

# Channels protected from being targeted by any tool
PROTECTED_CHANNELS = {'trustedzoneofficials', 'trustedzone', 'trustedzone_official'}

def parse_channel_username(raw: str) -> str:
    """Extract plain channel username from any input format.

    Handles:
      @username, username, t.me/username, https://t.me/username,
      https://t.me/username/123  (post URL — returns just username)
    """
    s = str(raw).strip()
    s = s.lstrip('@')
    if 't.me/' in s:
        # Remove scheme
        s = s.replace('https://', '').replace('http://', '')
        parts = s.strip('/').split('/')
        # parts = ['t.me', 'username'] or ['t.me', 'username', '123']
        if len(parts) >= 2:
            s = parts[1]
    return s.strip()


def is_protected_target(target: str) -> bool:
    """Return True if the target is a protected channel/group."""
    if not target:
        return False
    t = parse_channel_username(target).lower()
    return t in PROTECTED_CHANNELS


def get_user_subscription(user_id):
    sub = Subscription.query.filter_by(user_id=user_id).first()
    if not sub:
        return {'plan': 'free', 'active': True, 'expires_at': None, 'expired': False}
    if sub.plan != 'unlimited' and sub.expires_at and sub.expires_at < datetime.utcnow():
        return {'plan': 'free', 'active': False, 'expires_at': sub.expires_at, 'expired': True,
                'original_plan': sub.plan}
    return {'plan': sub.plan, 'active': True, 'expires_at': sub.expires_at, 'expired': False}


def require_plan(min_plan):
    """Return error dict if current user doesn't have required plan, else None."""
    order = ['free', 'basic', 'pro', 'unlimited']
    sub = get_user_subscription(current_user.id)
    plan = sub['plan']
    if order.index(plan) < order.index(min_plan):
        return {'success': False, 'error': f'This feature requires {min_plan.capitalize()} plan or higher. Your plan: {plan.capitalize()}', 'upgrade_required': True}
    return None


def check_account_limit():
    """Return error dict if user exceeded their account limit."""
    sub = get_user_subscription(current_user.id)
    limit = PLAN_LIMITS[sub['plan']]['accounts']
    count = TelegramAccount.query.filter_by(user_id=current_user.id).count()
    if count >= limit:
        return {'success': False, 'error': f'Account limit reached ({count}/{limit}). Upgrade your plan to add more accounts.', 'upgrade_required': True}
    return None


def generate_license_key():
    chars = string.ascii_uppercase + string.digits
    parts = [''.join(secrets.choice(chars) for _ in range(5)) for _ in range(4)]
    return '-'.join(parts)


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _database_reachable():
    """Cheap liveness probe: tells 'database down' apart from 'schema drift'."""
    from sqlalchemy import text as sa_text
    try:
        db.session.execute(sa_text('SELECT 1'))
        return True
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return False


def _fallback_user_lookup(column, value):
    """Raw-SQL user lookup that ignores columns added by newer migrations.

    If the ORM query fails — e.g. because a migration could not run on this
    database — an administrator must still be able to sign in. Only the core
    columns (id, username, password) are selected.
    """
    from sqlalchemy import text as sa_text
    if column not in ('id', 'username'):
        return None
    for table_name in ('"user"', 'user'):
        try:
            rows = db.session.execute(
                sa_text(f'SELECT id, username, password FROM {table_name} WHERE {column} = :value'),
                {'value': value},
            ).fetchall()
        except Exception:
            try:
                db.session.rollback()
            except Exception:
                pass
            continue
        if not rows:
            return None
        row = rows[0]
        stub = User(username=row[1], password=row[2])
        stub.id = row[0]
        stub.is_banned = False
        stub.last_login_at = None
        return stub
    return None


@login_manager.user_loader
def load_user(user_id):
    """Resolve the session cookie. Must NEVER raise.

    Any failure here (database unreachable, table missing, psycopg2 absent,
    malformed cookie) used to bubble up as HTTP 500 on EVERY page — including
    /login, which is why the login screen showed "Internal server error".
    A failure is now logged and the visitor is simply treated as logged out.
    """
    try:
        numeric_id = safe_int(user_id, 0)
        if numeric_id <= 0:
            return None
        return User.query.get(numeric_id)
    except Exception as e:
        logger.warning('ORM user lookup failed (%s) — retrying with a raw SQL lookup.', e)
        return _fallback_user_lookup('id', safe_int(user_id, 0))


def get_api_credentials(user_id=None):
    if user_id:
        try:
            settings = Settings.query.filter_by(user_id=user_id).first()
            if settings and settings.api_id and settings.api_hash:
                return int(settings.api_id), settings.api_hash
        except Exception:
            pass
    api_id = os.environ.get('TELEGRAM_API_ID', '')
    api_hash = os.environ.get('TELEGRAM_API_HASH', '')
    if not api_id or not api_hash or api_id in ('your-api-id-from-my.telegram.org', 'your-api-id'):
        return None, None
    try:
        return int(api_id), api_hash
    except ValueError:
        return None, None


def get_proxy_for_account(user_id, account_id=None):
    """Return (proxy_tuple, rdns) or None."""
    if not SOCKS_AVAILABLE:
        return None
    try:
        cfg = None
        if account_id:
            cfg = ProxyConfig.query.filter_by(user_id=user_id, account_id=account_id, is_active=True).first()
        if not cfg:
            cfg = ProxyConfig.query.filter_by(user_id=user_id, account_id=None, is_active=True).first()
        if not cfg:
            return None
        proxy_type_map = {
            'socks5': socks.SOCKS5,
            'socks4': socks.SOCKS4,
            'http': socks.HTTP,
        }
        ptype = proxy_type_map.get(cfg.proxy_type, socks.SOCKS5)
        if cfg.username and cfg.password:
            return (ptype, cfg.host, cfg.port, True, cfg.username, cfg.password)
        return (ptype, cfg.host, cfg.port)
    except Exception:
        return None


def make_client(session_str, api_id, api_hash, proxy=None):
    return TelegramClient(StringSession(session_str), api_id, api_hash, proxy=proxy)


# ─── ADMIN HELPERS ────────────────────────────────────────────────────────────

def is_admin_user():
    """True if the currently logged-in user is an admin."""
    return current_user.is_authenticated and current_user.username in ADMIN_USERS


def get_admin_user_id():
    """Return the admin User.id (first admin in DB), or None."""
    admin = User.query.filter_by(username='admin').first()
    return admin.id if admin else None


def get_account_for_current_user(account_id):
    """Return a TelegramAccount the current user is allowed to access.
    Admin can access any account; regular users only their own."""
    if is_admin_user():
        return TelegramAccount.query.get(account_id)
    return TelegramAccount.query.filter_by(user_id=current_user.id, id=account_id).first()


def get_accounts_for_current_user():
    """Return all TelegramAccount rows accessible to the current user.
    Admin gets every account from every user."""
    if is_admin_user():
        return TelegramAccount.query.all()
    return TelegramAccount.query.filter_by(user_id=current_user.id).all()


def resolve_account_and_creds(account_id):
    """Return (account, api_id, api_hash, proxy) for an account_id.
    Automatically uses the account owner's API credentials, so admin can
    operate any user's account with that user's own api_id/api_hash."""
    account = get_account_for_current_user(account_id)
    if not account:
        return None, None, None, None
    api_id, api_hash = get_api_credentials(account.user_id)
    proxy = get_proxy_for_account(account.user_id, account_id)
    return account, api_id, api_hash, proxy


def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def log_activity(user_id, action, status='success', account_id=None, details=None):
    try:
        with app.app_context():
            log = ActivityLog(
                user_id=user_id,
                action=action,
                status=status,
                account_id=account_id,
                details=details
            )
            db.session.add(log)
            db.session.commit()
    except Exception as e:
        logger.error(f"Error logging activity: {e}")


# ─── DURABLE TASK PROGRESS (survives Railway restarts) ───────────────────────

def _task_cache(task_key):
    return task_progress.setdefault(
        task_key,
        {'status': 'running', 'sent': 0, 'failed': 0, 'skipped': 0, 'total': 0, 'errors': []},
    )


def task_run_start(user_id, kind, total=0, task_key=None):
    """Create/refresh a durable TaskRun row and return its task key."""
    key = task_key or uuid.uuid4().hex
    cache = _task_cache(key)
    cache.update({'status': 'running', 'sent': 0, 'failed': 0, 'skipped': 0, 'total': int(total or 0), 'errors': []})
    try:
        with app.app_context():
            row = TaskRun.query.filter_by(task_key=key).first()
            if row is None:
                row = TaskRun(task_key=key, user_id=user_id, kind=kind)
                db.session.add(row)
            row.kind = kind
            row.status = 'running'
            row.sent = 0
            row.failed = 0
            row.skipped = 0
            row.total = int(total or 0)
            row.errors = None
            row.updated_at = datetime.utcnow()
            db.session.commit()
    except Exception as e:
        logger.warning('task_run_start(%s) could not be persisted: %s', kind, e)
    return key


def task_run_update(task_key, **fields):
    """Update the in-memory cache and the durable TaskRun row together."""
    cache = _task_cache(task_key)
    new_errors = fields.pop('errors', None)
    if isinstance(new_errors, (list, tuple)):
        cache.setdefault('errors', []).extend(str(e) for e in new_errors if e)
    elif new_errors:
        cache.setdefault('errors', []).append(str(new_errors))
    cache.update({k: v for k, v in fields.items() if v is not None})
    try:
        with app.app_context():
            row = TaskRun.query.filter_by(task_key=task_key).first()
            if row is None:
                return cache
            for field, value in fields.items():
                if value is not None and hasattr(row, field):
                    setattr(row, field, value)
            if cache.get('errors'):
                row.errors = '\n'.join(cache['errors'][-20:])
            row.updated_at = datetime.utcnow()
            db.session.commit()
    except Exception as e:
        logger.debug('task_run_update(%s) not persisted: %s', task_key, e)
    return cache


def task_run_finish(task_key, status='completed', **fields):
    """Mark a durable task finished (completed/partial/failed/stopped)."""
    # A stop the operator requested always wins over the worker's own final status,
    # otherwise a stopped run would report back as 'completed' and the UI would not
    # show it as stopped.
    if task_should_stop(task_key):
        status = 'stopped'
        task_clear_stop(task_key)
    return task_run_update(task_key, status=status, **fields)


# ── Stop a running background task ───────────────────────────────────────────
# Broadcast / bulk DM / campaign / forwarder runs used to be unstoppable: with a long
# delay the operator could only watch "Sent: 1/64" for hours. The worker now checks
# this set between sends, so Stop takes effect after the message in flight.
TASK_CANCEL = set()
TASK_CANCEL_LOCK = threading.Lock()


def task_request_stop(task_key):
    """Ask a running task to stop."""
    with TASK_CANCEL_LOCK:
        TASK_CANCEL.add(task_key)
    try:
        task_run_update(task_key, status='stopped')
    except Exception as e:
        logger.warning('task_request_stop(%s) could not be recorded: %s', task_key, e)
    return True


def task_should_stop(task_key):
    with TASK_CANCEL_LOCK:
        return task_key in TASK_CANCEL


def task_clear_stop(task_key):
    with TASK_CANCEL_LOCK:
        TASK_CANCEL.discard(task_key)


# ── Durable broadcast rounds ─────────────────────────────────────────────────
# "Start Now" used to run its whole round loop inside the request thread: a full
# round over every group, then `delay` seconds of asyncio.sleep, then the next
# round. That only worked for as long as that one process stayed alive.
#
# On Railway a redeploy, a crash or a platform restart kills the thread while it
# is sitting in the multi-hour sleep. The campaign then stays 'running' forever,
# the progress bar is already full and round 2 never happens — which is exactly
# the "4 hours later nothing happens" report. The delay was never the problem.
#
# The round loop now lives in a background scheduler that starts with the other
# jobs at boot, and every round is booked in the DATABASE (rounds_done /
# next_round_at). A campaign therefore survives a restart: on boot the scheduler
# sees it is due (or overdue) and continues with the next round.
_BROADCAST_INFLIGHT = set()
_BROADCAST_INFLIGHT_LOCK = threading.Lock()
_BROADCAST_WAKE = threading.Event()
BROADCAST_TICK_SECONDS = max(5, _limit_env('BROADCAST_TICK_SECONDS', 15))


def broadcast_task_id_from_key(task_key):
    """'broadcast:12' -> 12 (the BroadcastTask row id), otherwise None."""
    head, _, tail = str(task_key or '').partition(':')
    if head != 'broadcast' or not tail:
        return None
    try:
        return int(tail)
    except (TypeError, ValueError):
        return None


def _broadcast_claim(task_id):
    """True only for the caller that wins the right to run this round."""
    with _BROADCAST_INFLIGHT_LOCK:
        if task_id in _BROADCAST_INFLIGHT:
            return False
        _BROADCAST_INFLIGHT.add(task_id)
        return True


def _broadcast_release(task_id):
    with _BROADCAST_INFLIGHT_LOCK:
        _BROADCAST_INFLIGHT.discard(task_id)


def stop_broadcast_task_for_key(task_key):
    """Stop the durable BroadcastTask behind a 'broadcast:N' task key.

    The scheduler re-books the next round from the database, so a stop that only
    touched the in-memory TASK_CANCEL set would leave the campaign queued and it
    would start up again on the next tick.
    """
    task_id = broadcast_task_id_from_key(task_key)
    if not task_id:
        return False
    try:
        with app.app_context():
            task = BroadcastTask.query.get(task_id)
            if not task:
                return False
            if (task.status or '') == 'running':
                task.status = 'stopped'
                task.next_round_at = None
                task.completed_at = datetime.utcnow()
                db.session.commit()
            return True
    except Exception as e:
        logger.warning('stop_broadcast_task_for_key(%s): %s', task_key, e)
        return False


def broadcast_run_one_round(task_id, already_claimed=False):
    """Send ONE round to every group of a BroadcastTask, then book the next round.

    next_round_at is written BEFORE the round starts, so a process that dies
    mid-round resumes after the interval instead of instantly re-sending a whole
    round to everyone. Returns the round result dict, or None when nothing to do.
    """
    if not already_claimed and not _broadcast_claim(task_id):
        return None
    try:
        with app.app_context():
            task = BroadcastTask.query.get(task_id)
            if not task or (task.status or '') != 'running':
                return None
            account = TelegramAccount.query.get(task.account_id)
            task_key = task.task_key or f'broadcast:{task_id}'
            if not account or not account.session_string \
                    or account.session_string in ('demo', 'active'):
                task.status = 'failed'
                task.next_round_at = None
                task.completed_at = datetime.utcnow()
                db.session.commit()
                task_run_finish(task_key, 'failed',
                                errors=['Account is not connected any more'])
                return None
            user_id = task.user_id
            account_id = task.account_id
            sess = account.session_string
            message_type = task.message_type or 'text'
            content = task.content or ''
            caption = task.caption
            repeat = safe_int(task.repeat, 1, minimum=0)
            rounds_done = safe_int(task.rounds_done, 0)
            between_rounds = max(60, safe_int(
                task.delay, 60, minimum=1, maximum=MAX_DELAY_SECONDS))
            lifetime_sent = safe_int(task.sent_count, 0)
            try:
                group_ids = json.loads(task.groups or '[]')
            except (TypeError, ValueError):
                group_ids = []
            if not group_ids:
                task.status = 'failed'
                task.next_round_at = None
                task.completed_at = datetime.utcnow()
                db.session.commit()
                task_run_finish(task_key, 'failed', errors=['No groups left to send to'])
                return None
            api_id, api_hash = get_api_credentials(user_id)
            proxy = get_proxy_for_account(user_id, account_id)
            lock = get_account_lock(account_id)
            # Book the NEXT round now. Two things depend on it: the scheduler stops
            # seeing this campaign as "due" while the round is in flight, and a
            # crash mid-round resumes after the interval instead of re-sending.
            task.next_round_at = datetime.utcnow() + timedelta(seconds=between_rounds)
            db.session.commit()

        if not api_id:
            with app.app_context():
                t = BroadcastTask.query.get(task_id)
                if t:
                    t.status = 'failed'
                    t.next_round_at = None
                    db.session.commit()
                task_run_finish(task_key, 'failed',
                                errors=['Telegram API credentials not configured'])
            return None

        result = {'sent': 0, 'failed': 0, 'skipped': 0, 'errors': []}

        async def _one_round():
            client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
            await client.connect()
            try:
                # Inside a round the messages go out one after another with only a
                # 1s safety gap, so the whole round finishes quickly. The operator's
                # delay is the wait BETWEEN rounds.
                for group_id in group_ids:
                    if task_should_stop(task_key):
                        break
                    try:
                        entity = await client.get_entity(safe_int(group_id, 0) or group_id)
                        if message_type in ('photo', 'video'):
                            await client.send_file(entity, content, caption=caption or content)
                        else:
                            await client.send_message(entity, content)
                        result['sent'] += 1
                        task_run_update(task_key, sent=result['sent'], total=len(group_ids))
                        await asyncio.sleep(1)
                    except FloodWaitError as e:
                        # Honour Telegram's wait — never bypass the rate limit.
                        wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                        result['skipped'] += 1
                        note = f'rate limit — waited {wait}s, target skipped this round'
                        result['errors'].append(note)
                        task_run_update(task_key, skipped=result['skipped'], errors=[note])
                        await asyncio.sleep(wait)
                    except Exception as e:
                        result['failed'] += 1
                        note = friendly_telegram_error(e, str(group_id))
                        result['errors'].append(note)
                        task_run_update(task_key, failed=result['failed'], errors=[note])
            finally:
                await client.disconnect()
            return result

        with lock:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(_one_round())
            finally:
                loop.close()

        stopped = task_should_stop(task_key)
        with app.app_context():
            task = BroadcastTask.query.get(task_id)
            if not task:
                return result
            rounds_done = safe_int(task.rounds_done, 0) + 1
            task.rounds_done = rounds_done
            task.sent_count = safe_int(task.sent_count, 0) + result['sent']
            lifetime_sent = task.sent_count
            # rounds=0 means "keep going forever"; rounds=N means N rounds total.
            more_rounds = (not stopped) and (repeat == 0 or rounds_done < repeat)
            if more_rounds:
                task.status = 'running'
                task.next_round_at = datetime.utcnow() + timedelta(seconds=between_rounds)
            else:
                if stopped:
                    final_status = 'stopped'
                elif result['sent'] and not result['failed']:
                    final_status = 'completed'
                elif not result['sent']:
                    final_status = 'failed'
                else:
                    final_status = 'partial'
                task.status = final_status
                task.next_round_at = None
                task.completed_at = datetime.utcnow()
            acc = TelegramAccount.query.get(account_id)
            if acc and result['sent']:
                acc.messages_sent_today = safe_int(acc.messages_sent_today, 0) + result['sent']
                acc.total_messages = safe_int(acc.total_messages, 0) + result['sent']
                acc.last_used = datetime.utcnow()
            db.session.commit()
            next_round_at = task.next_round_at
            final_status = task.status

        logger.info('Broadcast %s: round %s done (%s sent this round, %s lifetime), next round %s',
                    task_id, rounds_done, result['sent'], lifetime_sent,
                    next_round_at.isoformat() if next_round_at else 'not scheduled')
        if more_rounds:
            # Keep the progress card alive so the operator can see the wait.
            task_run_update(task_key, status='running', sent=0, failed=0, skipped=0,
                            total=len(group_ids))
            task_run_update(task_key, errors=result['errors'])
        else:
            task_run_finish(task_key, final_status, sent=result['sent'],
                            failed=result['failed'], skipped=result['skipped'])
            log_activity(user_id,
                         f"Broadcast #{task_id} {final_status}: {lifetime_sent} sent over "
                         f"{rounds_done} round(s)",
                         'success' if result['sent'] else 'failed', account_id)
        if result['errors']:
            logger.info('Broadcast %s issues: %s', task_id, ' | '.join(result['errors'][-5:]))
        return result
    except Exception as e:
        logger.exception('broadcast_run_one_round(%s) failed: %s', task_id, e)
        try:
            with app.app_context():
                t = BroadcastTask.query.get(task_id)
                if t and (t.status or '') == 'running':
                    t.status = 'failed'
                    t.next_round_at = None
                    t.completed_at = datetime.utcnow()
                    db.session.commit()
                task_run_finish(f'broadcast:{task_id}', 'failed',
                                errors=[friendly_telegram_error(e)])
        except Exception:
            pass
        return None
    finally:
        _broadcast_release(task_id)


def close_stranded_legacy_campaigns():
    """Close 'running' campaigns left behind by the old in-memory round loop.

    Those rows have no task_key and no round bookkeeping, so nothing knows which
    round they reached or whether the operator still wants them. Resuming one
    would fire an unexpected round at every group it lists, so they are closed
    instead and the operator restarts them deliberately from the dashboard.
    Returns how many were closed.
    """
    closed = 0
    try:
        with app.app_context():
            legacy = (BroadcastTask.query
                      .filter(BroadcastTask.status == 'running',
                              BroadcastTask.task_key.is_(None))
                      .all())
            for t in legacy:
                t.status = 'stopped'
                t.next_round_at = None
                t.completed_at = t.completed_at or datetime.utcnow()
                closed += 1
            if closed:
                db.session.commit()
    except Exception as e:
        logger.error('close_stranded_legacy_campaigns: %s', e)
    return closed


def run_broadcast_scheduler():
    """Durable runner for Start-Now broadcast rounds. Survives restarts.

    Every tick it looks for campaigns that are 'running' and due (or overdue, or
    never booked because the process died before the first round). One thread per
    due campaign; `_BROADCAST_INFLIGHT` makes sure a slow round is never started
    twice.
    """
    logger.info('broadcast_scheduler started (tick %ss)', BROADCAST_TICK_SECONDS)
    closed = close_stranded_legacy_campaigns()
    if closed:
        logger.info('broadcast_scheduler: closed %s stranded pre-upgrade campaign(s) '
                    'instead of resuming them', closed)
    while True:
        try:
            _BROADCAST_WAKE.wait(timeout=BROADCAST_TICK_SECONDS)
            _BROADCAST_WAKE.clear()
            now = datetime.utcnow()
            with app.app_context():
                rows = (BroadcastTask.query
                        .filter(BroadcastTask.status == 'running')
                        .order_by(BroadcastTask.id.asc())
                        .limit(25).all())
                due = [r.id for r in rows
                       if r.next_round_at is None or r.next_round_at <= now]
            for task_id in due:
                # Claim here so a slow round is never spawned twice from two ticks.
                if not _broadcast_claim(task_id):
                    continue
                threading.Thread(target=broadcast_run_one_round,
                                 args=(task_id, True), daemon=True).start()
        except Exception as e:
            logger.error('broadcast_scheduler: %s', e)
        time.sleep(1)


@app.route('/bot/broadcast/campaigns')
@login_required
def broadcast_campaigns():
    """Broadcast runs of this user, newest first — the same idea as the Auto Video
    Poster campaign list, so a campaign stays visible (and removable) after it starts."""
    rows = (TaskRun.query
            .filter(TaskRun.user_id == current_user.id,
                    TaskRun.task_key.like('broadcast:%'))
            .order_by(TaskRun.id.desc())
            .limit(25).all())
    # Round bookkeeping lives on BroadcastTask, not on the progress row. Joining it
    # in lets the card say "round 2 of 10 — next round in 3h 59m" instead of leaving
    # the operator staring at a full progress bar wondering whether it is still alive.
    ids = [i for i in (broadcast_task_id_from_key(r.task_key) for r in rows) if i]
    meta = {}
    if ids:
        for t in BroadcastTask.query.filter(BroadcastTask.id.in_(ids)).all():
            meta[t.id] = t
    now = datetime.utcnow()
    out = []
    for r in rows:
        age = None
        if r.created_at:
            age = max(0, int((now - r.created_at).total_seconds()))
        errs = [e.strip() for e in (r.errors or '').splitlines() if e.strip()]
        bt = meta.get(broadcast_task_id_from_key(r.task_key))
        item = {
            'id': r.id,
            'task_key': r.task_key,
            'status': r.status or 'running',
            'sent': r.sent or 0,
            'failed': r.failed or 0,
            'skipped': r.skipped or 0,
            'total': r.total or 0,
            'errors': errs[-3:],
            'created_at': r.created_at.isoformat() if r.created_at else None,
            'age_seconds': age,
            'rounds_done': 0,
            'rounds_total': 1,
            'next_round_in': None,
            'lifetime_sent': 0,
            'targets': 0,
        }
        if bt:
            item['rounds_done'] = safe_int(bt.rounds_done, 0)
            item['rounds_total'] = safe_int(bt.repeat, 1)
            item['lifetime_sent'] = safe_int(bt.sent_count, 0)
            try:
                item['targets'] = len(json.loads(bt.groups or '[]'))
            except (TypeError, ValueError):
                item['targets'] = 0
            if bt.next_round_at:
                item['next_round_in'] = int((bt.next_round_at - now).total_seconds())
        out.append(item)
    return jsonify({'success': True, 'campaigns': out})


@app.route('/bot/broadcast/campaigns/<int:run_id>', methods=['DELETE'])
@login_required
def broadcast_campaign_delete(run_id):
    """Remove a broadcast from the list. A running one is stopped first."""
    row = TaskRun.query.filter_by(id=run_id, user_id=current_user.id).first()
    if not row:
        return jsonify({'success': False, 'message': 'Not found'}), 404
    was_running = (row.status or '') == 'running'
    key = row.task_key
    if was_running:
        task_request_stop(key)
    # Drop the durable campaign too, otherwise the scheduler would keep booking
    # rounds for a campaign the operator has just deleted.
    _bt_id = broadcast_task_id_from_key(key)
    if _bt_id:
        _bt = BroadcastTask.query.get(_bt_id)
        if _bt:
            db.session.delete(_bt)
    db.session.delete(row)
    db.session.commit()
    try:
        task_progress.pop(key, None)
    except Exception:
        pass
    log_activity(current_user.id, f'Deleted broadcast campaign ({key})', 'info')
    return jsonify({
        'success': True,
        'message': 'Removed.' + (' The running broadcast was stopped first.' if was_running else ''),
    })


@app.route('/bot/task/<path:task_key>/stop', methods=['POST'])
@login_required
def stop_task(task_key):
    """Stop a running task. Safe to call twice."""
    task_request_stop(task_key)
    # A broadcast's next round is re-booked from the database, so a stop has to
    # clear the durable row as well — otherwise the campaign comes straight back
    # on the next scheduler tick.
    stop_broadcast_task_for_key(task_key)
    log_activity(current_user.id, f'Stop requested for {task_key}', 'info')
    return jsonify({
        'success': True,
        'message': 'Stop requested — the run ends after the message already in flight.',
    })


def task_run_snapshot(task_key):
    """Best-known progress for a task: durable row first, live cache merged in."""
    cache = task_progress.get(task_key)
    row = None
    try:
        with app.app_context():
            row = TaskRun.query.filter_by(task_key=task_key).first()
    except Exception:
        row = None
    if row is None:
        return cache
    db_state = {
        'status': row.status,
        'sent': row.sent or 0,
        'failed': row.failed or 0,
        'skipped': row.skipped or 0,
        'total': row.total or 0,
        'errors': (row.errors or '').splitlines()[-20:],
        'persisted': True,
        'updated_at': row.updated_at.strftime('%Y-%m-%d %H:%M:%S') if row.updated_at else None,
    }
    if not cache:
        return db_state
    merged = dict(db_state)
    merged.update({k: v for k, v in cache.items() if v not in (None, [], '')})
    return merged


def next_repeat_settings(repeat_interval, repeat_count):
    """Decide whether a finished scheduled broadcast should repeat.

    UI contract: repeat_count == 0 means "∞" (repeat forever); repeat_count == N
    means N runs in total. The previous implementation decremented the counter and
    then always rescheduled, so a finite count silently became an endless loop.
    Returns (should_repeat, remaining_count_for_the_next_run).
    """
    interval = safe_int(repeat_interval, 0)
    count = safe_int(repeat_count, 0)
    if interval <= 0:
        return False, 0
    if count == 0:
        return True, 0
    remaining = count - 1
    return remaining > 0, remaining


def parse_post_url(url):
    url = url.strip()
    m = re.match(r'https?://t\.me/c/(\d+)/(\d+)', url)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.match(r'https?://t\.me/([^/]+)/(\d+)', url)
    if m:
        return m.group(1), int(m.group(2))
    return url, None


# ─── AUTH ROUTES ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return redirect(url_for('login'))

@app.route('/ping')
def ping():
    """No-auth diagnostic endpoint."""
    return jsonify({'ok': True, 'msg': 'pong'})


def _render_login(error_message=None, status=200):
    """Render the login page; fall back to an inline form if the template is missing.

    A deployment that forgot to include `templates/` used to answer every page
    with a bare "Internal server error". Now the operator always gets a usable
    login form plus a pointer to /diag.
    """
    try:
        return render_template('login.html', error=error_message), status
    except Exception as e:
        logger.exception('login.html could not be rendered: %s', e)
        return _inline_login_page(error_message), status


def _inline_login_page(error_message=None):
    """Built-in login form (used only when templates/login.html is unavailable)."""
    message = ('<div class="err">%s</div>' % html_escape(error_message)) if error_message else ''
    return '''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Login — MR ADS TG Automation</title><style>
body{font-family:'Inter',system-ui,Segoe UI,sans-serif;background:#070b14;color:#e8eefc;min-height:100vh;
display:flex;align-items:center;justify-content:center;padding:20px;margin:0}
.box{width:100%;max-width:400px;background:rgba(20,28,45,.8);border:1px solid rgba(120,150,200,.18);
border-radius:16px;padding:34px;box-shadow:0 30px 60px -30px rgba(0,0,0,.9)}
h1{font-size:20px;margin:0 0 6px;color:#22d3ee}p.sub{color:#8fa0bf;font-size:12.5px;margin:0 0 20px}
label{display:block;font-size:12.5px;color:#8fa0bf;margin:14px 0 6px}
input{width:100%;padding:12px 14px;border-radius:11px;background:rgba(6,10,20,.85);color:#e8eefc;
border:1px solid rgba(120,150,200,.18);font-size:14px;box-sizing:border-box}
input:focus{outline:none;border-color:rgba(34,211,238,.6)}
button{width:100%;margin-top:20px;padding:13px;border:none;border-radius:11px;font-weight:700;font-size:14.5px;
background:linear-gradient(100deg,#22d3ee,#3b9dff);color:#04121c;cursor:pointer}
.err{margin:14px 0;padding:11px 13px;border-radius:10px;background:rgba(255,107,129,.12);
border:1px solid rgba(255,107,129,.35);color:#ffb3c0;font-size:13px}
.note{margin-top:18px;font-size:11.5px;color:#6c7d9c;line-height:1.6}
a{color:#22d3ee}</style></head><body><div class="box">
<h1>⚡ MR ADS TG Automation</h1><p class="sub">Fallback login page — the HTML template was not found in this deployment.</p>
''' + message + '''
<form method="POST" action="/login">
<label for="username">Username</label><input id="username" name="username" autocomplete="username" required>
<label for="password">Password</label><input id="password" name="password" type="password" autocomplete="current-password" required>
<button type="submit">Sign in</button></form>
<p class="note">Fix the deployment by committing the <code>templates/</code> folder
(login.html, dashboard.html, admin.html). Diagnostics: <a href="/diag">/diag</a></p>
</div></body></html>'''


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = clean_str(request.form.get('username'))
        # The account row is always 'admin' (ADMIN_USERS, the admin panel and the
        # boot-time recovery all key off that exact name), but operators sign in
        # as "MRADS" — treat it as an alias instead of renaming the row, which
        # would make the app recreate a second 'admin' account on the next boot.
        if username.lower() in ('mrads', 'mrads_tg', 'mradstg'):
            username = 'admin'
        password = request.form.get('password') or ''
        try:
            user = User.query.filter_by(username=username).first()
        except Exception as e:
            # Distinguish "database is down" from "a column is missing on this
            # database" — the second must not lock the administrator out.
            logger.warning('login: ORM lookup failed: %s', e)
            if not _database_reachable():
                logger.error('login: the database is unreachable')
                return _render_login(
                    'The database is not reachable yet. Open /diag for details, or check the Railway logs.',
                    503,
                )
            user = _fallback_user_lookup('username', username)
        if user and bool(getattr(user, 'is_banned', False)):
            return _render_login('This account has been banned by the administrator.', 403)
        try:
            valid = bool(user and check_password_hash(user.password, password))
        except Exception:
            valid = False
        if valid:
            login_user(user, remember=True)
            try:
                user.last_login_at = datetime.utcnow()
                db.session.commit()
            except Exception:
                db.session.rollback()
            log_activity(user.id, 'Signed in', 'success')
            return redirect(url_for('dashboard'), 303)
        return _render_login('Invalid username or password', 401)
    return _render_login()


@app.route('/register', methods=['POST'])
def register():
    """Self-service registration — disabled unless ALLOW_REGISTRATION=1.

    This is an admin-grade Telegram automation panel, so open sign-up was a
    security hole. Admins can still create accounts from the Admin panel.
    """
    if not ALLOW_REGISTRATION:
        return jsonify({
            'error': 'Public registration is disabled. Ask an administrator to create your account, '
                     'or set ALLOW_REGISTRATION=1 to enable it.',
            'registration_disabled': True,
        }), 403
    try:
        username = clean_str(request.form.get('username'))
        password = request.form.get('password') or ''
        if not username or not password:
            return jsonify({'error': 'Username and password required'}), 400
        if len(username) < 3 or len(password) < 6:
            return jsonify({'error': 'Username must be 3+ characters and password 6+ characters'}), 400
        if User.query.filter_by(username=username).first():
            return jsonify({'error': 'Username already exists'}), 400
        user = User(username=username, password=generate_password_hash(password))
        db.session.add(user)
        db.session.commit()
        log_activity(user.id, f'Registered new dashboard user {username}', 'success')
        return jsonify({'success': True, 'message': 'Account created successfully'})
    except Exception as e:
        logger.error('register error: %s', e)
        return jsonify({'error': 'Could not create the account right now'}), 500


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    resp = make_response(render_template('dashboard.html', user=current_user))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp


# ─── SETTINGS / CREDENTIALS ──────────────────────────────────────────────────

@app.route('/bot/check_credentials')
@login_required
def check_credentials():
    api_id, api_hash = get_api_credentials(current_user.id)
    settings = Settings.query.filter_by(user_id=current_user.id).first()
    source = 'database' if (settings and settings.api_id) else ('env' if api_id else 'none')
    return jsonify({
        'configured': api_id is not None,
        'api_id': str(settings.api_id) if settings and settings.api_id else os.environ.get('TELEGRAM_API_ID', ''),
        'source': source
    })


@app.route('/bot/save_credentials', methods=['POST'])
@login_required
def save_credentials():
    try:
        data = request.get_json()
        api_id_str = str(data.get('api_id', '')).strip()
        api_hash_str = str(data.get('api_hash', '')).strip()
        if not api_id_str or not api_hash_str:
            return jsonify({'success': False, 'error': 'Both API ID and API Hash are required'})
        try:
            int(api_id_str)
        except ValueError:
            return jsonify({'success': False, 'error': 'API ID must be a number'})
        settings = Settings.query.filter_by(user_id=current_user.id).first()
        if not settings:
            settings = Settings(user_id=current_user.id)
            db.session.add(settings)
        settings.api_id = api_id_str
        settings.api_hash = api_hash_str
        db.session.commit()
        log_activity(current_user.id, "Saved Telegram API credentials", 'success')
        return jsonify({'success': True, 'message': 'API credentials saved successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── PROXY SETTINGS ──────────────────────────────────────────────────────────

@app.route('/bot/save_proxy', methods=['POST'])
@login_required
def save_proxy():
    try:
        data = request.get_json()
        proxy_type = data.get('proxy_type', 'socks5')
        host = data.get('host', '').strip()
        port = safe_int(data.get('port'), 0)
        username = data.get('username', '').strip() or None
        password = data.get('password', '').strip() or None
        account_id = data.get('account_id') or None
        if account_id:
            account_id = int(account_id)

        if not host or not port:
            return jsonify({'success': False, 'error': 'Host and port required'})

        cfg = ProxyConfig.query.filter_by(user_id=current_user.id, account_id=account_id).first()
        if not cfg:
            cfg = ProxyConfig(user_id=current_user.id, account_id=account_id)
            db.session.add(cfg)
        cfg.proxy_type = proxy_type
        cfg.host = host
        cfg.port = port
        cfg.username = username
        cfg.password = password
        cfg.is_active = True
        db.session.commit()
        return jsonify({'success': True, 'message': 'Proxy saved'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/get_proxy')
@login_required
def get_proxy():
    try:
        cfgs = ProxyConfig.query.filter_by(user_id=current_user.id).all()
        # Build account id→phone map for friendly display
        accts = TelegramAccount.query.filter_by(user_id=current_user.id).all()
        acct_map = {a.id: a.phone for a in accts}
        return jsonify({'success': True, 'proxies': [{
            'id': c.id,
            'account_id': c.account_id,
            'phone': acct_map.get(c.account_id, '') if c.account_id else '',
            'proxy_type': c.proxy_type,
            'host': c.host,
            'port': c.port,
            'username': c.username,
            'is_active': c.is_active
        } for c in cfgs]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/delete_proxy/<int:proxy_id>', methods=['DELETE'])
@login_required
def delete_proxy(proxy_id):
    try:
        cfg = ProxyConfig.query.get_or_404(proxy_id)
        if cfg.user_id != current_user.id:
            return jsonify({'error': 'Unauthorized'}), 403
        db.session.delete(cfg)
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── ACCOUNT MANAGEMENT ──────────────────────────────────────────────────────

@app.route('/bot/send_code', methods=['POST'])
@login_required
def send_code():
    try:
        # Check account limit based on plan
        limit_err = check_account_limit()
        if limit_err:
            return jsonify(limit_err)

        data = request.get_json()
        phone = data.get('phone', '').strip()
        if not phone:
            return jsonify({'success': False, 'error': 'Phone number required'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured. Please enter your API ID and API Hash above.'})

        existing = TelegramAccount.query.filter_by(user_id=current_user.id, phone=phone).first()
        if existing:
            return jsonify({'success': False, 'error': 'This phone number is already added'})

        proxy = get_proxy_for_account(current_user.id)

        async def _send():
            client = TelegramClient(StringSession(), api_id, api_hash, proxy=proxy)
            await client.connect()
            result = await client.send_code_request(phone)
            session_after = client.session.save()
            await client.disconnect()
            return result.phone_code_hash, session_after

        phone_code_hash, session_after = run_async(_send())
        pending_logins[phone] = {
            'phone_code_hash': phone_code_hash,
            'session_after_send': session_after,
            'user_id': current_user.id,
            'api_id': api_id,
            'api_hash': api_hash
        }

        return jsonify({'success': True, 'message': f'Verification code sent to {phone}'})

    except Exception as e:
        logger.error(f"send_code error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/verify_code', methods=['POST'])
@login_required
def verify_code():
    try:
        data = request.get_json()
        phone = data.get('phone', '').strip()
        code = data.get('code', '').strip()

        if not phone or not code:
            return jsonify({'success': False, 'error': 'Phone and code required'})

        if phone not in pending_logins:
            return jsonify({'success': False, 'error': 'No pending login for this phone. Send code first.'})

        info = pending_logins[phone]
        api_id = info['api_id']
        api_hash = info['api_hash']
        phone_code_hash = info['phone_code_hash']
        proxy = get_proxy_for_account(current_user.id)

        async def _verify():
            client = TelegramClient(StringSession(info['session_after_send']), api_id, api_hash, proxy=proxy)
            await client.connect()
            try:
                await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
                session_string = client.session.save()
                me = await client.get_me()
                await client.disconnect()
                return {'done': True, 'session': session_string, 'name': f"{me.first_name or ''} {me.last_name or ''}".strip(), 'username': me.username}
            except SessionPasswordNeededError:
                partial = client.session.save()
                await client.disconnect()
                return {'done': False, 'needs_2fa': True, 'partial_session': partial}

        result = run_async(_verify())

        if result.get('needs_2fa'):
            pending_logins[phone]['partial_session'] = result['partial_session']
            return jsonify({'success': True, 'needs_2fa': True, 'message': '2FA password required'})

        account = TelegramAccount(
            user_id=current_user.id,
            phone=phone,
            session_string=result['session'],
            is_active=True
        )
        db.session.add(account)
        db.session.commit()
        del pending_logins[phone]

        log_activity(current_user.id, f"Connected Telegram account {phone} ({result.get('name', '')})", 'success', account.id)
        return jsonify({'success': True, 'message': f"Account {phone} connected successfully!"})

    except PhoneCodeInvalidError:
        return jsonify({'success': False, 'error': 'Invalid verification code. Please try again.'})
    except PhoneCodeExpiredError:
        if phone in pending_logins:
            del pending_logins[phone]
        return jsonify({'success': False, 'error': 'Code expired. Please click "Send Code" again.'})
    except Exception as e:
        logger.error(f"verify_code error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/verify_2fa', methods=['POST'])
@login_required
def verify_2fa():
    try:
        data = request.get_json()
        phone = data.get('phone', '').strip()
        password = data.get('password', '').strip()

        if phone not in pending_logins or 'partial_session' not in pending_logins[phone]:
            return jsonify({'success': False, 'error': 'Session expired. Please start over.'})

        info = pending_logins[phone]
        api_id = info['api_id']
        api_hash = info['api_hash']
        partial_session = info['partial_session']
        proxy = get_proxy_for_account(current_user.id)

        async def _2fa():
            client = TelegramClient(StringSession(partial_session), api_id, api_hash, proxy=proxy)
            await client.connect()
            await client.sign_in(password=password)
            session_string = client.session.save()
            me = await client.get_me()
            await client.disconnect()
            return session_string, f"{me.first_name or ''} {me.last_name or ''}".strip()

        session_string, name = run_async(_2fa())

        account = TelegramAccount(
            user_id=current_user.id,
            phone=phone,
            session_string=session_string,
            is_active=True
        )
        db.session.add(account)
        db.session.commit()
        del pending_logins[phone]

        log_activity(current_user.id, f"Connected Telegram account {phone} ({name}) with 2FA", 'success', account.id)
        return jsonify({'success': True, 'message': f"Account {phone} connected successfully!"})

    except Exception as e:
        logger.error(f"verify_2fa error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/remove_account/<int:account_id>', methods=['DELETE'])
@login_required
def remove_account(account_id):
    try:
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        phone = account.phone
        db.session.delete(account)
        db.session.commit()
        log_activity(current_user.id, f"Removed account {phone}", 'success')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── EXPORT / IMPORT ACCOUNTS ────────────────────────────────────────────────

@app.route('/bot/export_accounts')
@login_required
def export_accounts():
    """Export all account session data as JSON backup."""
    try:
        accounts = get_accounts_for_current_user()
        creds = Settings.query.filter_by(user_id=current_user.id).first()
        proxies = ProxyConfig.query.filter_by(user_id=current_user.id).all()
        data = {
            'version': 1,
            'credentials': {
                'api_id': creds.api_id if creds else '',
                'api_hash': creds.api_hash if creds else ''
            },
            'proxies': [{
                'proxy_type': p.proxy_type, 'host': p.host, 'port': p.port,
                'username': p.username or '', 'password': p.password or '',
                'account_id': p.account_id
            } for p in proxies],
            'accounts': [{
                'phone': a.phone,
                'session_string': a.session_string,
                'is_active': a.is_active,
                'total_messages': a.total_messages,
                'total_reports': a.total_reports,
                'members_scraped': a.members_scraped,
                'members_added': a.members_added
            } for a in accounts if a.session_string and a.session_string not in ('demo', 'active')]
        }
        data['warning'] = ('This file contains Telegram session strings — treat it like a password. '
                           'Never commit it to GitHub and never share it publicly.')
        data['exported_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        # Dated filename so repeated downloads never overwrite an older backup.
        filename = 'tgbot_backup_' + datetime.utcnow().strftime('%Y%m%d_%H%M') + '.json'
        resp = make_response(json.dumps(data, indent=2))
        resp.headers['Content-Type'] = 'application/json'
        resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        resp.headers['X-Content-Type-Options'] = 'nosniff'
        log_activity(
            current_user.id,
            f"Exported account backup ({len(data['accounts'])} accounts"
            + (", admin scope: all users" if is_admin_user() else "") + ")",
            'success',
        )
        return resp
    except Exception as e:
        logger.error('export_accounts error: %s', e)
        return jsonify({'success': False, 'error': 'Export failed — see server logs.'}), 500


@app.route('/bot/import_accounts', methods=['POST'])
@login_required
def import_accounts():
    """Import accounts from JSON backup. Skips duplicates by phone number."""
    try:
        data = request.get_json()
        if not data or data.get('version') != 1:
            return jsonify({'success': False, 'error': 'Invalid backup file format'})

        imported = 0
        skipped = 0
        errors = []

        # Restore API credentials if not already set
        creds_data = data.get('credentials', {})
        if creds_data.get('api_id') and creds_data.get('api_hash'):
            existing_creds = Settings.query.filter_by(user_id=current_user.id).first()
            if not existing_creds:
                db.session.add(Settings(
                    user_id=current_user.id,
                    api_id=str(creds_data['api_id']),
                    api_hash=str(creds_data['api_hash'])
                ))

        # Restore proxies
        for proxy_data in data.get('proxies', []):
            if not proxy_data.get('host'):
                continue
            existing_proxy = ProxyConfig.query.filter_by(
                user_id=current_user.id,
                host=proxy_data['host'],
                port=safe_int(proxy_data.get('port'), 1080)
            ).first()
            if not existing_proxy:
                db.session.add(ProxyConfig(
                    user_id=current_user.id,
                    proxy_type=proxy_data.get('proxy_type', 'socks5').lower(),
                    host=proxy_data['host'],
                    port=safe_int(proxy_data.get('port'), 1080),
                    username=proxy_data.get('username') or None,
                    password=proxy_data.get('password') or None,
                    account_id=proxy_data.get('account_id')
                ))

        # Restore accounts
        for acc_data in data.get('accounts', []):
            phone = acc_data.get('phone', '').strip()
            session = acc_data.get('session_string', '').strip()
            if not phone or not session:
                continue
            existing = TelegramAccount.query.filter_by(user_id=current_user.id, phone=phone).first()
            if existing:
                skipped += 1
                continue
            try:
                db.session.add(TelegramAccount(
                    user_id=current_user.id,
                    phone=phone,
                    session_string=session,
                    is_active=acc_data.get('is_active', True),
                    total_messages=acc_data.get('total_messages', 0),
                    total_reports=acc_data.get('total_reports', 0),
                    members_scraped=acc_data.get('members_scraped', 0),
                    members_added=acc_data.get('members_added', 0)
                ))
                imported += 1
            except Exception as e:
                errors.append(f"{phone}: {str(e)}")

        db.session.commit()
        log_activity(current_user.id, f"Imported {imported} accounts ({skipped} skipped)", 'success')
        return jsonify({
            'success': True,
            'imported': imported,
            'skipped': skipped,
            'errors': errors,
            'message': f'✅ {imported} accounts imported, {skipped} already existed'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── ALERT & HEALTH MONITOR ──────────────────────────────────────────────────

def _send_tg_alert(token, chat_id, text):
    """Send a Telegram message via Bot API (no Telethon needed)."""
    if not HTTP_REQUESTS_OK or not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        http_requests.post(url, json={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}, timeout=10)
        return True
    except Exception as e:
        logger.error(f"TG alert failed: {e}")
        return False


def _send_email_alert(settings, subject, body):
    """Send an email alert via SMTP."""
    if not settings or not settings.email_enabled or not settings.email_to:
        return False
    try:
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = settings.smtp_user or 'tgbot@alert'
        msg['To'] = settings.email_to
        with smtplib.SMTP(settings.smtp_host or 'smtp.gmail.com', settings.smtp_port or 587, timeout=15) as s:
            s.starttls()
            if settings.smtp_user and settings.smtp_pass:
                s.login(settings.smtp_user, settings.smtp_pass)
            s.sendmail(msg['From'], [settings.email_to], msg.as_string())
        return True
    except Exception as e:
        logger.error(f"Email alert failed: {e}")
        return False


def _fire_alerts(user_id, account_phone, status, detail):
    """Fire both TG + email alerts for an account status change."""
    try:
        with app.app_context():
            al = AlertSettings.query.filter_by(user_id=user_id).first()
            if not al:
                return
            emoji = {'limited': '⚠️', 'banned': '🚫', 'deactivated': '💀', 'auth_error': '🔑'}.get(status, '❓')
            text = (
                f"{emoji} <b>Account Alert</b>\n"
                f"Phone: <code>{account_phone}</code>\n"
                f"Status: <b>{status.upper()}</b>\n"
                f"Detail: {detail}\n"
                f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            )
            if al.tg_enabled and al.tg_bot_token and al.tg_chat_id:
                _send_tg_alert(al.tg_bot_token, al.tg_chat_id, text)
            if al.email_enabled and al.email_to:
                plain = text.replace('<b>', '').replace('</b>', '').replace('<code>', '').replace('</code>', '')
                _send_email_alert(al, f"TG Bot Alert: {account_phone} {status}", plain)
    except Exception as e:
        logger.error(f"_fire_alerts error: {e}")


async def _check_single_account_health(session_str, api_id, api_hash, proxy=None):
    """Connect briefly, determine health status and return (status, detail, tg_name, tg_username)."""
    status = 'unknown'
    detail = ''
    tg_name = ''
    tg_username = ''
    client = TelegramClient(StringSession(session_str), api_id, api_hash, proxy=proxy)
    try:
        await asyncio.wait_for(client.connect(), timeout=20)
        if not await client.is_user_authorized():
            status = 'auth_error'
            detail = 'Session not authorized — account may be logged out or banned'
        else:
            me = await client.get_me()
            if me is None:
                status = 'auth_error'
                detail = 'Could not fetch account info'
            else:
                tg_name     = ' '.join(filter(None, [getattr(me, 'first_name', ''), getattr(me, 'last_name', '')])).strip()
                tg_username = getattr(me, 'username', '') or ''
                if getattr(me, 'restricted', False):
                    reasons = getattr(me, 'restriction_reason', []) or []
                    detail_str = ', '.join(str(r) for r in reasons) if reasons else 'Restricted by Telegram'
                    status = 'limited'
                    detail = detail_str
                else:
                    status = 'ok'
                    detail = 'Account active'
    except (UserDeactivatedError, UserDeactivatedBanError) as e:
        status = 'banned'
        detail = f'Account deactivated/banned: {e}'
    except (AuthKeyUnregisteredError, AuthKeyDuplicatedError) as e:
        status = 'auth_error'
        detail = f'Auth key error: {e}'
    except FloodWaitError as e:
        status = 'limited'
        detail = f'FloodWait {e.seconds}s — rate limited by Telegram'
    except asyncio.TimeoutError:
        status = 'unknown'
        detail = 'Connection timeout — check proxy or network'
    except Exception as e:
        status = 'unknown'
        detail = str(e)[:200]
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
    return status, detail, tg_name, tg_username


async def _spambot_release(session_str, api_id, api_hash, proxy=None):
    """Attempt to release account limit via @SpamBot. Returns (success, response_text)."""
    client = TelegramClient(StringSession(session_str), api_id, api_hash, proxy=proxy)
    try:
        await asyncio.wait_for(client.connect(), timeout=20)
        if not await client.is_user_authorized():
            return False, 'Session not authorized'
        async with client.conversation('@SpamBot', timeout=30) as conv:
            await conv.send_message('/start')
            response = await conv.get_response()
            resp_text = response.raw_text or ''
            # If SpamBot has buttons (e.g. "No, it's a mistake"), click first button
            if response.buttons:
                for row in response.buttons:
                    for btn in row:
                        try:
                            await btn.click()
                            break
                        except Exception:
                            pass
                    break
                try:
                    final = await conv.get_response()
                    resp_text = final.raw_text or resp_text
                except Exception:
                    pass
            good_keywords = ['no limits', 'good news', 'not limited', 'appealed']
            success = any(kw.lower() in resp_text.lower() for kw in good_keywords)
            return success, resp_text[:300]
    except Exception as e:
        return False, str(e)[:200]
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def run_health_check_all(user_id, api_id, api_hash):
    """Background: check all accounts for a user and update health DB."""
    with app.app_context():
        accounts = TelegramAccount.query.filter_by(user_id=user_id).all()
        for acc in accounts:
            if not acc.session_string or acc.session_string in ('demo', 'active'):
                continue
            try:
                proxy = get_proxy_for_account(user_id, acc.id)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                status, detail, tg_name, tg_username = loop.run_until_complete(
                    _check_single_account_health(acc.session_string, api_id, api_hash, proxy)
                )

                health = AccountHealth.query.filter_by(account_id=acc.id).first()
                prev_status = health.status if health else 'unknown'
                if not health:
                    health = AccountHealth(account_id=acc.id, user_id=user_id)
                    db.session.add(health)
                health.status    = status
                health.detail    = detail
                health.checked_at= datetime.utcnow()
                if tg_name:     health.tg_name     = tg_name
                if tg_username: health.tg_username  = tg_username

                # Auto SpamBot release if account is limited and not yet attempted
                if status == 'limited' and not health.spambot_released:
                    try:
                        sp_ok, sp_text = loop.run_until_complete(
                            _spambot_release(acc.session_string, api_id, api_hash, proxy)
                        )
                        health.spambot_released = True
                        health.spambot_result   = sp_text[:300]
                        log_activity(user_id, f"SpamBot release {acc.phone}: {'OK' if sp_ok else 'pending'} — {sp_text[:80]}", 'success' if sp_ok else 'warning', acc.id)
                    except Exception as sp_e:
                        health.spambot_result = str(sp_e)[:200]

                loop.close()

                # Fire alert only when status worsens and not yet alerted
                bad_statuses = {'limited', 'banned', 'deactivated', 'auth_error'}
                if status in bad_statuses and prev_status != status:
                    health.alerted = False
                if status in bad_statuses and not health.alerted:
                    health.alerted = True
                    db.session.commit()
                    _fire_alerts(user_id, acc.phone, status, detail)
                else:
                    db.session.commit()
                log_activity(user_id, f"Health check {acc.phone}: {status}", 'success' if status == 'ok' else 'warning', acc.id)
            except Exception as e:
                logger.error(f"Health check error for {acc.phone}: {e}")
        logger.info(f"Health check complete for user {user_id}")


def schedule_health_monitor():
    """Background thread: runs health checks every 30 minutes for all users."""
    def _loop():
        while True:
            time.sleep(1800)  # 30 minutes
            try:
                with app.app_context():
                    users = User.query.all()
                    for user in users:
                        creds = Settings.query.filter_by(user_id=user.id).first()
                        if creds and creds.api_id and creds.api_hash:
                            threading.Thread(
                                target=run_health_check_all,
                                args=(user.id, creds.api_id, creds.api_hash),
                                daemon=True
                            ).start()
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    threading.Thread(target=_loop, daemon=True).start()


@app.route('/bot/save_alert_settings', methods=['POST'])
@login_required
def save_alert_settings():
    try:
        data = request.get_json()
        al = AlertSettings.query.filter_by(user_id=current_user.id).first()
        if not al:
            al = AlertSettings(user_id=current_user.id)
            db.session.add(al)
        al.tg_bot_token  = data.get('tg_bot_token', '').strip() or None
        al.tg_chat_id    = data.get('tg_chat_id', '').strip() or None
        al.email_to      = data.get('email_to', '').strip() or None
        al.smtp_host     = data.get('smtp_host', 'smtp.gmail.com').strip() or 'smtp.gmail.com'
        al.smtp_port     = safe_int(data.get('smtp_port'), 587, minimum=1, maximum=65535)
        al.smtp_user     = data.get('smtp_user', '').strip() or None
        al.smtp_pass     = data.get('smtp_pass', '').strip() or None
        al.tg_enabled    = bool(data.get('tg_enabled'))
        al.email_enabled = bool(data.get('email_enabled'))
        al.updated_at    = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'message': 'Alert settings saved'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/get_alert_settings')
@login_required
def get_alert_settings():
    try:
        al = AlertSettings.query.filter_by(user_id=current_user.id).first()
        if not al:
            return jsonify({'success': True, 'settings': {
                'tg_bot_token': '', 'tg_chat_id': '', 'tg_enabled': False,
                'email_to': '', 'smtp_host': 'smtp.gmail.com', 'smtp_port': 587,
                'smtp_user': '', 'smtp_pass': '', 'email_enabled': False
            }})
        return jsonify({'success': True, 'settings': {
            'tg_bot_token': al.tg_bot_token or '',
            'tg_chat_id':   al.tg_chat_id or '',
            'tg_enabled':   al.tg_enabled,
            'email_to':     al.email_to or '',
            'smtp_host':    al.smtp_host or 'smtp.gmail.com',
            'smtp_port':    al.smtp_port or 587,
            'smtp_user':    al.smtp_user or '',
            'smtp_pass':    al.smtp_pass or '',
            'email_enabled': al.email_enabled
        }})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/check_health_now', methods=['POST'])
@login_required
def check_health_now():
    """Manually trigger health check for all accounts (admin: all users)."""
    try:
        if is_admin_user():
            # Trigger health check for every user that has API credentials
            users = User.query.all()
            started = 0
            for u in users:
                a_id, a_hash = get_api_credentials(u.id)
                if a_id:
                    threading.Thread(target=run_health_check_all, args=(u.id, a_id, a_hash), daemon=True).start()
                    started += 1
            return jsonify({'success': True, 'message': f'Health check started for {started} user(s)'})
        else:
            api_id, api_hash = get_api_credentials(current_user.id)
            if not api_id:
                return jsonify({'success': False, 'error': 'API credentials not set'})
            threading.Thread(target=run_health_check_all, args=(current_user.id, api_id, api_hash), daemon=True).start()
            return jsonify({'success': True, 'message': 'Health check started in background'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/account_health')
@login_required
def account_health():
    """Return latest health status for all accounts (admin: all users)."""
    try:
        accounts = get_accounts_for_current_user()
        health_map = {}
        health_q = AccountHealth.query.all() if is_admin_user() else AccountHealth.query.filter_by(user_id=current_user.id).all()
        for h in health_q:
            health_map[h.account_id] = h
        result = []
        for acc in accounts:
            h = health_map.get(acc.id)
            result.append({
                'account_id': acc.id,
                'phone': acc.phone,
                'status': h.status if h else 'unchecked',
                'detail': h.detail if h else '',
                'checked_at': h.checked_at.isoformat() if h else None
            })
        return jsonify({'success': True, 'health': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/spambot_release/<int:account_id>', methods=['POST'])
@login_required
def spambot_release_manual(account_id):
    """Manually trigger SpamBot release for a specific account."""
    try:
        acc = TelegramAccount.query.get_or_404(account_id)
        if acc.user_id != current_user.id:
            return jsonify({'error': 'Unauthorized'}), 403
        api_id, api_hash = get_api_credentials(acc.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not set'})
        proxy = get_proxy_for_account(acc.user_id, account_id)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ok, text = loop.run_until_complete(_spambot_release(acc.session_string, api_id, api_hash, proxy))
        loop.close()
        # Save result
        health = AccountHealth.query.filter_by(account_id=account_id).first()
        if health:
            health.spambot_released = True
            health.spambot_result   = text[:300]
            db.session.commit()
        log_activity(current_user.id, f"Manual SpamBot release {acc.phone}: {text[:80]}", 'success' if ok else 'warning', account_id)
        return jsonify({'success': True, 'released': ok, 'message': text})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/test_alert', methods=['POST'])
@login_required
def test_alert():
    """Send a test alert to verify notification settings."""
    try:
        data = request.get_json()
        kind = data.get('kind', 'tg')
        al = AlertSettings.query.filter_by(user_id=current_user.id).first()
        if not al:
            return jsonify({'success': False, 'error': 'Save alert settings first'})
        msg = f"✅ Test Alert\nYour TG Bot notification is working!\nTime: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
        if kind == 'tg':
            ok = _send_tg_alert(al.tg_bot_token, al.tg_chat_id, msg)
            return jsonify({'success': ok, 'message': 'Test message sent' if ok else 'Failed — check token and chat ID'})
        elif kind == 'email':
            ok = _send_email_alert(al, 'TG Bot Test Alert', msg)
            return jsonify({'success': ok, 'message': 'Test email sent' if ok else 'Failed — check SMTP settings'})
        return jsonify({'success': False, 'error': 'Unknown kind'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500



# ─── JOIN REQUESTS ────────────────────────────────────────────────────────────
# Admin-only Telegram join-request management for groups/supergroups/channels.
# Uses Telegram's MTProto messages.getChatInviteImporters / hideChatJoinRequest.
@app.route('/bot/join_requests/<int:account_id>')
@login_required
def get_join_requests(account_id):
    """Return pending join requests for a selected group/channel."""
    try:
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account is not properly connected'}), 400

        chat_id = request.args.get('chat_id', '').strip()
        if not chat_id:
            return jsonify({'success': False, 'error': 'chat_id is required'}), 400

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _get_requests():
            client = TelegramClient(
                StringSession(account.session_string),
                api_id,
                api_hash,
                proxy=proxy
            )
            await client.connect()
            try:
                peer = await client.get_input_entity(int(chat_id))
                result = await client(functions.messages.GetChatInviteImportersRequest(
                    requested=True,
                    peer=peer,
                    link=None,
                    q=None,
                    offset_date=0,
                    offset_user=types.InputUserEmpty(),
                    limit=100
                ))

                users_by_id = {int(u.id): u for u in getattr(result, 'users', [])}
                items = []
                for importer in getattr(result, 'importers', []):
                    uid = int(importer.user_id)
                    user = users_by_id.get(uid)
                    items.append({
                        'user_id': uid,
                        'access_hash': getattr(user, 'access_hash', None),
                        'first_name': getattr(user, 'first_name', '') or '',
                        'last_name': getattr(user, 'last_name', '') or '',
                        'username': getattr(user, 'username', None),
                        'phone': getattr(user, 'phone', None),
                        'about': getattr(importer, 'about', None),
                        'date': importer.date.isoformat() if getattr(importer, 'date', None) else None,
                    })

                return {
                    'success': True,
                    'count': len(items),
                    'total': int(getattr(result, 'count', len(items)) or len(items)),
                    'requests': items
                }
            finally:
                await client.disconnect()

        data = run_async(_get_requests())
        return jsonify(data)

    except Exception as e:
        logger.error(f"get_join_requests error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/join_requests/action_all', methods=['POST'])
@login_required
def join_requests_action_all():
    """Approve or dismiss all pending join requests for a selected chat."""
    payload = request.get_json(silent=True) or {}
    account_id = safe_int(payload.get('account_id'), 0)
    chat_id = str(payload.get('chat_id') or '').strip()
    approved = bool(payload.get('approved', True))

    if not account_id or not chat_id:
        return jsonify({'success': False, 'error': 'account_id and chat_id are required'}), 400

    account = get_account_for_current_user(account_id)
    if not account:
        return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
    if not account.session_string or account.session_string in ('demo', 'active'):
        return jsonify({'success': False, 'error': 'Account is not properly connected'}), 400

    api_id, api_hash = get_api_credentials(account.user_id)
    if not api_id:
        return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

    proxy = get_proxy_for_account(account.user_id, account_id)

    async def _action_all():
        client = TelegramClient(
            StringSession(account.session_string),
            api_id,
            api_hash,
            proxy=proxy
        )
        await client.connect()
        try:
            peer = await client.get_input_entity(int(chat_id))
            await client(functions.messages.HideAllChatJoinRequestsRequest(
                approved=approved,
                peer=peer,
                link=None
            ))
            return True
        finally:
            await client.disconnect()

    run_async(_action_all())
    account.last_used = datetime.utcnow()
    db.session.commit()

    return jsonify({
        'success': True,
        'approved': approved,
        'message': 'All pending join requests approved.' if approved else 'All pending join requests dismissed.'
    })


@app.route('/bot/join_requests/action', methods=['POST'])
@login_required
def action_join_request():
    """Approve or dismiss one pending Telegram join request."""
    try:
        payload = request.get_json(silent=True) or {}
        account_id = safe_int(payload.get('account_id'), 0)
        chat_id = str(payload.get('chat_id') or '').strip()
        user_id = safe_int(payload.get('user_id'), 0)
        approved = bool(payload.get('approved', True))

        if not account_id or not chat_id or not user_id:
            return jsonify({'success': False, 'error': 'account_id, chat_id and user_id are required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account is not properly connected'}), 400

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _action():
            client = TelegramClient(
                StringSession(account.session_string),
                api_id,
                api_hash,
                proxy=proxy
            )
            await client.connect()
            try:
                peer = await client.get_input_entity(int(chat_id))
                try:
                    user = await client.get_input_entity(user_id)
                except (ValueError, TypeError):
                    # The pending-request response may contain the user but the
                    # session cache may not have that user's access_hash. Re-fetch
                    # the pending importers and build InputUser from Telegram's
                    # returned User object.
                    result = await client(functions.messages.GetChatInviteImportersRequest(
                        requested=True,
                        peer=peer,
                        link=None,
                        q=None,
                        offset_date=0,
                        offset_user=types.InputUserEmpty(),
                        limit=100
                    ))
                    matched = next((u for u in (getattr(result, 'users', []) or [])
                                    if int(getattr(u, 'id', 0)) == user_id), None)
                    candidate_hash = getattr(matched, 'access_hash', None) if matched else None
                    if candidate_hash is None:
                        raise ValueError(
                            f'Could not resolve Telegram access_hash for user {user_id}. '
                            'Reload the pending requests and try again.'
                        )
                    user = types.InputUser(user_id=user_id, access_hash=int(candidate_hash))

                await client(functions.messages.HideChatJoinRequestRequest(
                    peer=peer,
                    user_id=user,
                    approved=approved
                ))
                return True
            finally:
                await client.disconnect()

        run_async(_action())
        account.last_used = datetime.utcnow()
        db.session.commit()

        return jsonify({
            'success': True,
            'approved': approved,
            'message': 'Join request approved' if approved else 'Join request dismissed'
        })

    except Exception as e:
        logger.error(f"action_join_request error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── GROUPS ──────────────────────────────────────────────────────────────────

@app.route('/bot/get_groups/<int:account_id>')
@login_required
def get_groups(account_id):
    try:
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'error': 'Account not properly connected. Please re-add this account.'}), 400

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _get_groups():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            me = await client.get_me()
            groups = []
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    ent = dialog.entity
                    is_broadcast = getattr(ent, 'broadcast', False) and not getattr(ent, 'megagroup', False)
                    default_banned = getattr(ent, 'default_banned_rights', None)
                    msgs_banned = getattr(default_banned, 'send_messages', False) if default_banned else False
                    can_send = not is_broadcast and not msgs_banned
                    groups.append({
                        'id': dialog.id,
                        'title': dialog.title,
                        'username': getattr(ent, 'username', None),
                        'participants_count': getattr(ent, 'participants_count', 0) or 0,
                        'is_channel': dialog.is_channel,
                        'is_broadcast': is_broadcast,
                        'can_send': can_send
                    })
            await client.disconnect()
            return groups

        groups = run_async(_get_groups())
        account.last_used = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'groups': groups, 'count': len(groups)})

    except Exception as e:
        logger.error(f"get_groups error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/bot/scrape_group_links/<int:account_id>')
@login_required
def scrape_group_links(account_id):
    try:
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'error': 'Account not properly connected'}), 400

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _scrape_links():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            links = []
            async for dialog in client.iter_dialogs():
                ent = dialog.entity
                # Only groups/supergroups — skip broadcast channels and private chats
                is_megagroup = getattr(ent, 'megagroup', False)
                is_broadcast  = getattr(ent, 'broadcast', False) and not is_megagroup
                is_regular_group = isinstance(ent, types.Chat)
                if not (dialog.is_group or is_megagroup or is_regular_group):
                    continue
                if is_broadcast:
                    continue  # skip pure channels
                # Skip groups with fewer than 20 members
                members_count = getattr(ent, 'participants_count', 0) or 0
                if members_count < 20:
                    continue
                # Skip groups where messaging is turned off for regular members
                ban_rights = getattr(ent, 'default_banned_rights', None)
                if ban_rights and getattr(ban_rights, 'send_messages', False):
                    continue
                # Build link fast — prefer public username, skip heavy invite export
                username = getattr(ent, 'username', None)
                if username:
                    link = f'https://t.me/{username}'
                else:
                    link = f'(private, ID: {dialog.id})'
                links.append({
                    'id': dialog.id,
                    'title': dialog.title,
                    'link': link,
                    'type': 'Supergroup' if is_megagroup else 'Group',
                    'members': members_count
                })
            await client.disconnect()
            return links

        links = run_async(_scrape_links())
        account.last_used = datetime.utcnow()
        db.session.commit()
        log_activity(current_user.id, f"Scraped {len(links)} group links", 'success', account_id)
        return jsonify({'success': True, 'links': links, 'count': len(links)})

    except Exception as e:
        logger.error(f"scrape_group_links error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/search_keyword_groups', methods=['POST'])
@login_required
def search_keyword_groups():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        keyword = data.get('keyword', '').strip()
        limit = safe_int(data.get('limit'), 30, minimum=1, maximum=MAX_HISTORY_LIMIT)
        join_ids = data.get('join_ids', [])

        if not keyword:
            return jsonify({'success': False, 'error': 'Keyword required'})

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _search_and_join():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()

            # Build a set of already-joined group usernames/ids
            my_joined_usernames = set()
            my_joined_ids = set()
            async for dialog in client.iter_dialogs():
                my_joined_ids.add(str(dialog.id))
                if hasattr(dialog.entity, 'username') and dialog.entity.username:
                    my_joined_usernames.add(dialog.entity.username.lower())

            groups = []
            try:
                result = await client(functions.contacts.SearchRequest(q=keyword, limit=limit))
                for chat in result.chats:
                    username = getattr(chat, 'username', None)
                    is_broadcast = getattr(chat, 'broadcast', False)
                    already_joined = (
                        (username and username.lower() in my_joined_usernames) or
                        str(chat.id) in my_joined_ids
                    )
                    if is_protected_target(username):
                        continue
                    groups.append({
                        'id': chat.id,
                        'title': getattr(chat, 'title', 'Unknown'),
                        'username': username,
                        'participants_count': getattr(chat, 'participants_count', 0) or 0,
                        'is_channel': is_broadcast,
                        'type': 'Channel' if is_broadcast else 'Group',
                        'link': f'https://t.me/{username}' if username else None,
                        'already_joined': already_joined
                    })
            except Exception as e:
                logger.error(f"search error: {e}")

            joined = 0
            join_errors = []
            for jid in join_ids:
                try:
                    target_chat = next((g for g in groups if str(g['id']) == str(jid) or g['username'] == jid), None)
                    if target_chat and target_chat.get('username'):
                        ent = await client.get_entity(target_chat['username'])
                        await client(functions.channels.JoinChannelRequest(channel=ent))
                        joined += 1
                        await asyncio.sleep(2)
                except UserAlreadyParticipantError:
                    joined += 1
                except Exception as e:
                    join_errors.append(str(e))

            await client.disconnect()
            return groups, joined, join_errors

        groups, joined, join_errors = run_async(_search_and_join())
        account.last_used = datetime.utcnow()
        db.session.commit()

        msg = f"Found {len(groups)} results for '{keyword}'"
        if join_ids:
            msg += f" | Joined: {joined}, Failed: {len(join_errors)}"
        log_activity(current_user.id, msg, 'success', account_id)
        return jsonify({'success': True, 'results': groups, 'count': len(groups), 'joined': joined, 'join_errors': join_errors})

    except Exception as e:
        logger.error(f"search_keyword_groups error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/export_groups', methods=['POST'])
@login_required
def export_groups():
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        accounts = get_accounts_for_current_user()
        if account_ids:
            accounts = [a for a in accounts if a.id in [safe_int(x, 0) for x in account_ids]]

        all_groups = {}
        for account in accounts:
            if not account.session_string or account.session_string in ('demo', 'active'):
                continue
            try:
                proxy = get_proxy_for_account(account.user_id, account.id)
                async def _get(sess=account.session_string, acc_id=account.id):
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=get_proxy_for_account(account.user_id, acc_id))
                    await client.connect()
                    groups = []
                    async for dialog in client.iter_dialogs():
                        if dialog.is_group or dialog.is_channel:
                            ent = dialog.entity
                            username = getattr(ent, 'username', None)
                            groups.append({
                                'id': dialog.id,
                                'title': dialog.title,
                                'username': username,
                                'link': f'https://t.me/{username}' if username else f'(ID: {dialog.id})',
                                'is_channel': dialog.is_channel,
                                'participants': getattr(ent, 'participants_count', 0) or 0
                            })
                    await client.disconnect()
                    return groups
                groups = run_async(_get())
                for g in groups:
                    key = str(g['id'])
                    if key not in all_groups:
                        all_groups[key] = g
                account.last_used = datetime.utcnow()
            except Exception as e:
                logger.error(f"export_groups account {account.phone} error: {e}")

        db.session.commit()
        result = sorted(all_groups.values(), key=lambda x: x.get('title', ''))
        return jsonify({'success': True, 'groups': result, 'count': len(result)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── PERSONAL CHATS ──────────────────────────────────────────────────────────

@app.route('/bot/get_personal_chats/<int:account_id>')
@login_required
def get_personal_chats(account_id):
    try:
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'error': 'Account not properly connected'}), 400

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _get_chats():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            me = await client.get_me()
            chats = []
            async for dialog in client.iter_dialogs():
                entity = getattr(dialog, 'entity', None)
                # Guard against dialogs whose entity Telethon could not resolve.
                if dialog.is_user and entity is not None and not getattr(entity, 'bot', False):
                    user = entity
                    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or f"User {user.id}"
                    chats.append({
                        'id': user.id,
                        'access_hash': user.access_hash,
                        'name': name,
                        'username': user.username,
                        'phone': user.phone,
                        'unread': dialog.unread_count,
                        'last_message': dialog.message.message[:80] if dialog.message and dialog.message.message else '',
                    })
            await client.disconnect()
            return me.id, chats

        my_id, chats = run_async(_get_chats())
        account.last_used = datetime.utcnow()
        db.session.commit()
        return jsonify({'success': True, 'chats': chats, 'count': len(chats), 'my_id': my_id})

    except Exception as e:
        logger.error(f"get_personal_chats error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/send_personal_message', methods=['POST'])
@login_required
def send_personal_message():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        target_id = safe_int(data.get('target_id'), 0)
        access_hash = safe_int(data.get('access_hash'), 0) or None
        username = clean_str(data.get('username')) or None
        message = clean_str(data.get('message'))
        media_path = clean_str(data.get('media_path'))

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not target_id or (not message and not media_path):
            return jsonify({'success': False, 'error': 'Target and message (or a photo/video) required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'}), 400

        api_id, api_hash_val = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _send():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                async def _deliver():
                    if username:
                        # Username is the most reliable peer — Telegram resolves it directly
                        return await _deliver_media_or_text(client, username, message, media_path)
                    if access_hash:
                        # InputPeerUser avoids an entity lookup
                        peer = types.InputPeerUser(user_id=target_id, access_hash=access_hash)
                        return await _deliver_media_or_text(client, peer, message, media_path)
                    # Last resort: warm the dialog cache, then send by id
                    await client.get_dialogs(limit=1)
                    return await _deliver_media_or_text(client, target_id, message, media_path)

                try:
                    return await _deliver()
                except FloodWaitError as e:
                    # Wait the required time once, then retry this single message.
                    await asyncio.sleep(min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS))
                    return await _deliver()
            finally:
                await client.disconnect()

        run_async(_send())
        account.messages_sent_today += 1
        account.total_messages += 1
        account.last_used = datetime.utcnow()
        db.session.commit()
        log_activity(current_user.id, f"Sent personal message to user {target_id}", 'success', account_id)
        return jsonify({'success': True, 'message': 'Message sent'})

    except FloodWaitError as e:
        return jsonify({'success': False, 'error': f'Telegram rate limit — retry after {safe_int(getattr(e, "seconds", 0), 5)}s'}), 429
    except Exception as e:
        logger.error('send_personal_message error: %s', e)
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


# ─── CHAT HISTORY ────────────────────────────────────────────────────────────

@app.route('/bot/get_chat_history', methods=['POST'])
@login_required
def get_chat_history():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        user_id = safe_int(data.get('user_id'), 0)
        access_hash = safe_int(data.get('access_hash'), 0) or None
        username = clean_str(data.get('username')) or None
        limit = safe_int(data.get('limit'), 50, minimum=1, maximum=MAX_HISTORY_LIMIT)

        if not account_id or (not user_id and not username):
            return jsonify({'success': False, 'error': 'Account and chat are required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'}), 400

        api_id, api_hash_val = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _get_history():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            me = await client.get_me()
            try:
                if username:
                    peer = username
                elif access_hash:
                    peer = types.InputPeerUser(user_id=user_id, access_hash=int(access_hash))
                else:
                    peer = user_id
                messages = []
                async for msg in client.iter_messages(peer, limit=limit):
                    if msg.text or msg.message:
                        messages.append({
                            'id': msg.id,
                            'text': msg.text or msg.message or '',
                            'date': msg.date.strftime('%Y-%m-%d %H:%M') if msg.date else '',
                            'time': msg.date.strftime('%H:%M') if msg.date else '',
                            'from_me': msg.out,
                        })
                messages.reverse()
                return messages
            finally:
                await client.disconnect()

        messages = run_async(_get_history())
        return jsonify({'success': True, 'messages': messages, 'count': len(messages)})

    except FloodWaitError as e:
        return jsonify({'success': False, 'error': f'Telegram rate limit — retry after {safe_int(getattr(e, "seconds", 0), 5)}s'}), 429
    except Exception as e:
        logger.error('get_chat_history error: %s', e)
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


# ─── ACCOUNT TOOLS ────────────────────────────────────────────────────────────

@app.route('/bot/update_profile', methods=['POST'])
@login_required
def update_profile():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        first_name = data.get('first_name', '').strip()
        last_name = data.get('last_name', '').strip()
        bio = data.get('bio', '').strip()
        username = data.get('username', '').strip()

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _update():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            results = []
            try:
                if first_name or last_name is not None:
                    await client(functions.account.UpdateProfileRequest(
                        first_name=first_name or '',
                        last_name=last_name or '',
                        about=bio
                    ))
                    results.append('Name/Bio updated')
                if username:
                    await client(functions.account.UpdateUsernameRequest(username=username))
                    results.append(f'Username set to @{username}')
            finally:
                await client.disconnect()
            return results

        results = run_async(_update())
        log_activity(current_user.id, f"Profile updated for account {account_id}", 'success', account_id)
        return jsonify({'success': True, 'message': ', '.join(results) if results else 'Profile updated'})

    except Exception as e:
        logger.error(f"update_profile error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/get_account_info/<int:account_id>')
@login_required
def get_account_info(account_id):
    try:
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _get_info():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                me = await client.get_me()
                full = await client(functions.users.GetFullUserRequest(id='me'))
                return {
                    'id': me.id,
                    'first_name': me.first_name or '',
                    'last_name': me.last_name or '',
                    'username': me.username or '',
                    'phone': me.phone or '',
                    'bio': full.full_user.about or '',
                    'premium': getattr(me, 'premium', False),
                    'dc_id': me.photo.dc_id if me.photo else 'N/A',
                    'restricted': me.restricted or False,
                }
            finally:
                await client.disconnect()

        info = run_async(_get_info())
        return jsonify({'success': True, 'info': info})

    except Exception as e:
        logger.error(f"get_account_info error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/mark_all_read', methods=['POST'])
@login_required
def mark_all_read():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _mark():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            count = 0
            try:
                async for dialog in client.iter_dialogs():
                    if dialog.unread_count > 0:
                        await client.send_read_acknowledge(dialog.entity)
                        count += 1
            finally:
                await client.disconnect()
            return count

        count = run_async(_mark())
        log_activity(current_user.id, f"Marked {count} chats as read", 'success', account_id)
        return jsonify({'success': True, 'message': f'Marked {count} chats as read'})

    except Exception as e:
        logger.error(f"mark_all_read error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/set_online', methods=['POST'])
@login_required
def set_online():
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        online = bool(data.get('online', True))

        if not account_ids:
            accounts = TelegramAccount.query.filter_by(user_id=current_user.id, status='connected').all()
            account_ids = [a.id for a in accounts]

        api_id, api_hash_val = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not configured'})

        results = []
        for aid in account_ids:
            account = TelegramAccount.query.get(aid)
            if not account or account.user_id != current_user.id:
                continue
            if not account.session_string or account.session_string in ('demo', 'active'):
                continue
            proxy = get_proxy_for_account(current_user.id, aid)

            async def _set_online(sess=account.session_string, prx=proxy):
                client = TelegramClient(StringSession(sess), api_id, api_hash_val, proxy=prx)
                await client.connect()
                try:
                    await client(functions.account.UpdateStatusRequest(offline=not online))
                finally:
                    await client.disconnect()

            try:
                run_async(_set_online())
                results.append({'id': aid, 'phone': account.phone, 'status': 'ok'})
            except Exception as e:
                results.append({'id': aid, 'phone': account.phone, 'status': str(e)})

        status_word = 'Online' if online else 'Offline'
        log_activity(current_user.id, f"Set {len(results)} account(s) {status_word}", 'success')
        return jsonify({'success': True, 'message': f'{len(results)} account(s) set to {status_word}', 'results': results})

    except Exception as e:
        logger.error(f"set_online error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/forward_message', methods=['POST'])
@login_required
def forward_message():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        from_chat = clean_str(data.get('from_chat'))
        msg_id = safe_int(data.get('msg_id'), 0)
        to_chats = dedupe_preserve(clean_str_list(data.get('to_chats'), split_lines_only=True))

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not from_chat or not msg_id or not to_chats:
            return jsonify({'success': False, 'error': 'Source chat, message ID, and destinations required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'}), 400

        api_id, api_hash_val = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400
        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _forward():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            ok, fail, details = 0, 0, []
            try:
                src = await client.get_entity(parse_channel_target(from_chat)['value'] if parse_channel_target(from_chat) else from_chat)
                for dest in to_chats:
                    target = parse_channel_target(dest)
                    if not target:
                        fail += 1
                        details.append(f'{dest}: invalid destination')
                        continue
                    attempt = 0
                    while True:
                        try:
                            dst = await client.get_entity(target['value'])
                            await client.forward_messages(dst, msg_id, src)
                            ok += 1
                            await asyncio.sleep(2)
                            break
                        except FloodWaitError as e:
                            attempt += 1
                            wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                            await asyncio.sleep(wait)
                            if attempt >= 2:
                                fail += 1
                                details.append(f'{dest}: rate limit — skipped after waiting {wait}s')
                                break
                        except Exception as e:
                            fail += 1
                            details.append(friendly_telegram_error(e, dest))
                            break
            finally:
                await client.disconnect()
            return ok, fail, details

        ok, fail, details = run_async(_forward())
        if details:
            logger.info('forward_message issues: %s', ' | '.join(details[-5:]))
        log_activity(
            current_user.id,
            f"Forwarded msg {msg_id} to {ok} chats ({fail} failed)",
            'success' if ok else 'failed', account_id,
        )
        # Only a confirmed forward counts as success.
        if ok:
            resp = {'success': True, 'message': f'Forwarded to {ok} chats, {fail} failed'}
            if details:
                resp['errors'] = details
            return jsonify(resp)
        return jsonify({'success': False, 'error': 'Forwarding failed: ' + '; '.join(details[:3])}), 502

    except FloodWaitError as e:
        return jsonify({'success': False, 'error': f'Telegram rate limit — retry after {safe_int(getattr(e, "seconds", 0), 5)}s'}), 429
    except Exception as e:
        logger.error('forward_message error: %s', e)
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


# ─── BROADCAST ───────────────────────────────────────────────────────────────

@app.route('/bot/broadcast', methods=['POST'])
@login_required
def broadcast():
    try:
        data = request.get_json() or {}
        account_id = safe_int(data.get('account_id'), 0)
        message_type = clean_str(data.get('type'), 'text') or 'text'
        content = clean_str(data.get('content'))
        caption = clean_str(data.get('caption'))
        # Duplicate selections must never cause duplicate sends.
        group_ids = dedupe_preserve([clean_str(g) for g in (data.get('group_ids') or []) if clean_str(g)])
        delay = safe_int(data.get('delay'), 3, minimum=1, maximum=MAX_DELAY_SECONDS)

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not content:
            return jsonify({'success': False, 'error': 'Content required'}), 400
        if not group_ids:
            return jsonify({'success': False, 'error': 'Select at least one group'}), 400
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected. Please re-add it.'}), 400
        if account_busy(account_id):
            return jsonify({'success': False, 'error': 'Another campaign is already using this account. Wait for it to finish or stop it first.'}), 409

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        # One durable campaign per account at a time. account_busy() only covers a
        # round that is in flight; a campaign waiting between rounds does not hold the
        # account lock, so this closes that gap.
        already = (BroadcastTask.query
                   .filter_by(account_id=account_id, status='running').first())
        if already:
            return jsonify({
                'success': False,
                'error': f'Campaign #{already.id} is already running on this account. '
                         f'Stop it first.',
            }), 409

        # How many ROUNDS to send. 1 = one round over every group then finish (safe
        # default, and what every pre-existing campaign expects). 0 = keep repeating:
        # a full round of all groups, wait `delay`, then all groups again, until Stop.
        repeat = safe_int(data.get('repeat'), 1, minimum=0, maximum=100000)

        task = BroadcastTask(
            user_id=current_user.id,
            account_id=account_id,
            message_type=message_type,
            content=content,
            caption=caption,
            groups=json.dumps(group_ids),
            delay=delay,
            repeat=repeat,
            rounds_done=0,
            next_round_at=datetime.utcnow(),
            status='running',
        )
        db.session.add(task)
        # flush (not commit) so the row has its id, then put task_key in the SAME
        # commit. A row that is 'running' with a NULL task_key would be mistaken
        # for a stranded pre-upgrade campaign by the boot sweep.
        db.session.flush()
        task_id = task.id
        task_key = f'broadcast:{task_id}'
        task.task_key = task_key
        db.session.commit()
        task_run_start(current_user.id, 'broadcast', len(group_ids), task_key=task_key)

        # Round 1 starts right here so "Start Now" is instant. Rounds 2..N — and any
        # resume after a restart — are owned by the durable scheduler above, which
        # reads next_round_at out of the database. Claiming first means the scheduler
        # cannot start a second, duplicate round 1.
        if _broadcast_claim(task_id):
            threading.Thread(target=broadcast_run_one_round,
                             args=(task_id, True), daemon=True).start()
        _BROADCAST_WAKE.set()

        rounds_note = f'every {delay}s' if repeat == 0 else f'{repeat} round(s)'
        log_activity(current_user.id,
                     f'Broadcast #{task_id} started to {len(group_ids)} groups ({rounds_note})',
                     'success', account_id)
        return jsonify({
            'success': True,
            'message': f'Broadcast started to {len(group_ids)} groups',
            'task_id': task_id, 'task_key': task_key,
        })

    except Exception as e:
        logger.exception('broadcast error')
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


@app.route('/bot/task_status/<int:task_id>')
@login_required
def task_status(task_id):
    task = BroadcastTask.query.get(task_id)
    if task and task.user_id != current_user.id:
        return jsonify({'error': 'Unauthorized'}), 403
    # Durable record first so progress survives an app restart.
    progress = task_run_snapshot(f'broadcast:{task_id}') or task_progress.get(task_id, {})
    return jsonify({'success': True, 'progress': progress})


# ─── SCHEDULED BROADCAST ─────────────────────────────────────────────────────

@app.route('/bot/schedule_broadcast', methods=['POST'])
@login_required
def schedule_broadcast():
    try:
        data = request.get_json() or {}
        account_id = safe_int(data.get('account_id'), 0)
        message_type = clean_str(data.get('type'), 'text') or 'text'
        content = clean_str(data.get('content'))
        caption = clean_str(data.get('caption'))
        group_ids = dedupe_preserve([clean_str(g) for g in (data.get('group_ids') or []) if clean_str(g)])
        delay = safe_int(data.get('delay'), 3, minimum=1, maximum=MAX_DELAY_SECONDS)
        scheduled_at_str = clean_str(data.get('scheduled_at'))

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not content or not group_ids or not scheduled_at_str:
            return jsonify({'success': False, 'error': 'Content, groups, and schedule time required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403

        # Accepts "2026-01-31T21:00", "…Z" and offset-aware input; stored as naive UTC.
        scheduled_at = parse_datetime_utc(scheduled_at_str)
        if scheduled_at is None:
            return jsonify({'success': False, 'error': 'Invalid date/time format. Use YYYY-MM-DDTHH:MM'}), 400

        if scheduled_at <= datetime.utcnow():
            return jsonify({'success': False, 'error': 'Schedule time must be in the future'}), 400

        repeat_interval = safe_int(data.get('repeat_interval'), 0, minimum=0, maximum=MAX_REPEAT_INTERVAL_HOURS)
        repeat_count    = safe_int(data.get('repeat_count'), 0, minimum=0, maximum=MAX_REPEAT_COUNT)

        sb = ScheduledBroadcast(
            user_id=current_user.id,
            account_id=account_id,
            message_type=message_type,
            content=content,
            caption=caption or None,
            groups=json.dumps(group_ids),
            delay=delay,
            scheduled_at=scheduled_at,
            status='pending',
            repeat_interval=repeat_interval,
            repeat_count=repeat_count
        )
        db.session.add(sb)
        db.session.commit()

        repeat_info = f', repeat every {repeat_interval}h' if repeat_interval else ''
        log_activity(current_user.id, f"Scheduled broadcast for {scheduled_at.strftime('%Y-%m-%d %H:%M')} to {len(group_ids)} groups{repeat_info}", 'success', account_id)
        return jsonify({'success': True, 'message': f'Broadcast scheduled for {scheduled_at.strftime("%Y-%m-%d %H:%M UTC")}{repeat_info}', 'id': sb.id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/get_scheduled_broadcasts')
@login_required
def get_scheduled_broadcasts():
    try:
        items = ScheduledBroadcast.query.filter_by(user_id=current_user.id).order_by(ScheduledBroadcast.scheduled_at).all()
        result = []
        for sb in items:
            acc = TelegramAccount.query.get(sb.account_id)
            # A corrupt/legacy groups value must not break the whole list.
            try:
                groups_count = len(json.loads(sb.groups or '[]'))
            except Exception:
                groups_count = 0
            progress = task_run_snapshot(f'sched:{sb.id}') or {}
            result.append({
                'id': sb.id,
                'account': acc.phone if acc else 'Unknown',
                'message_type': sb.message_type,
                'content': (sb.content or '')[:80],
                'groups_count': groups_count,
                'delay': sb.delay,
                'scheduled_at': sb.scheduled_at.isoformat() if sb.scheduled_at else None,
                'status': sb.status,
                'sent_count': sb.sent_count,
                'repeat_interval': safe_int(getattr(sb, 'repeat_interval', 0), 0),
                'repeat_count': safe_int(getattr(sb, 'repeat_count', 0), 0),
                'failed': progress.get('failed', 0),
                'skipped': progress.get('skipped', 0),
                'errors': progress.get('errors', [])[-3:],
            })
        return jsonify({'success': True, 'scheduled': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/delete_scheduled/<int:sb_id>', methods=['DELETE'])
@login_required
def delete_scheduled(sb_id):
    try:
        sb = ScheduledBroadcast.query.get_or_404(sb_id)
        if sb.user_id != current_user.id:
            return jsonify({'error': 'Unauthorized'}), 403
        db.session.delete(sb)
        db.session.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── REPORTING ───────────────────────────────────────────────────────────────

@app.route('/bot/report', methods=['POST'])
@login_required
def report():
    try:
        data = request.get_json()
        target_url = data.get('url', '').strip()
        post_url = data.get('post_url', '').strip()
        report_type = data.get('type', 'spam')
        texts = data.get('texts', [])
        custom_text = data.get('custom_text', '').strip()
        account_ids = data.get('account_ids', [])
        reports_per_account = max(1, safe_int(data.get('reports_per_account'), 1))

        if not texts and custom_text:
            texts = [custom_text]
        if not texts:
            texts = [f"Reporting: {report_type}"]

        if not target_url:
            return jsonify({'success': False, 'error': 'Target URL required'}), 400
        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        reason_map = {
            'spam': types.InputReportReasonSpam(),
            'violence': types.InputReportReasonViolence(),
            'scam': types.InputReportReasonOther(),
            'copyright': types.InputReportReasonCopyright(),
            'child_abuse': types.InputReportReasonChildAbuse(),
            'pornography': types.InputReportReasonPornography(),
            'fake': types.InputReportReasonFake(),
        }
        reason = reason_map.get(report_type, types.InputReportReasonSpam())

        valid_accounts = []
        for aid in account_ids:
            acc = TelegramAccount.query.get(int(aid))
            if acc and acc.user_id == current_user.id and acc.session_string and acc.session_string not in ('demo', 'active'):
                valid_accounts.append(acc)

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts selected'}), 400

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {
            'status': 'running',
            'current_account': '',
            'completed': 0,
            'total': len(valid_accounts),
            'results': [],
            'errors': []
        }

        user_id = current_user.id
        account_ids_list = [a.id for a in valid_accounts]
        texts_list = list(texts)

        def run_report_sequential():
            with app.app_context():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = []
                text_idx = [0]

                for acc_id in account_ids_list:
                    acc = TelegramAccount.query.get(acc_id)
                    if not acc:
                        continue
                    task_progress[task_id]['current_account'] = acc.phone
                    proxy = get_proxy_for_account(user_id, acc_id)

                    async def _report_one(sess=acc.session_string, phone=acc.phone, t_idx=text_idx[0]):
                        client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                        await client.connect()
                        reported = 0
                        errors = []
                        try:
                            entity = await client.get_entity(target_url)
                            for i in range(reports_per_account):
                                msg_text = texts_list[(t_idx + i) % len(texts_list)]
                                try:
                                    await client(ReportRequest(
                                        peer=entity,
                                        id=[],
                                        reason=reason,
                                        message=msg_text
                                    ))
                                    reported += 1
                                except Exception as re:
                                    errors.append(f"channel report: {re}")
                                await asyncio.sleep(1)

                            if post_url:
                                try:
                                    channel_id, msg_id = parse_post_url(post_url)
                                    if msg_id:
                                        post_entity = await client.get_entity(channel_id)
                                        for i in range(reports_per_account):
                                            msg_text = texts_list[(t_idx + i) % len(texts_list)]
                                            try:
                                                await client(ReportRequest(
                                                    peer=post_entity,
                                                    id=[msg_id],
                                                    reason=reason,
                                                    message=msg_text
                                                ))
                                                reported += 1
                                            except Exception as re2:
                                                errors.append(f"post report: {re2}")
                                            await asyncio.sleep(1)
                                except Exception as pe:
                                    errors.append(f"post link: {pe}")
                        except Exception as e:
                            errors.append(str(e))
                        finally:
                            await client.disconnect()
                        return reported, errors

                    try:
                        reported, errors = loop.run_until_complete(_report_one())
                        acc.reports_sent_today += reported
                        acc.total_reports += reported
                        acc.last_used = datetime.utcnow()
                        db.session.commit()
                        result = {'account': acc.phone, 'success': True, 'reported': reported, 'errors': errors}
                        log_activity(user_id, f"Reported {target_url} using {acc.phone}: {reported} reports", 'success', acc.id)
                    except Exception as e:
                        result = {'account': acc.phone, 'success': False, 'reported': 0, 'errors': [str(e)]}
                        log_activity(user_id, f"Failed to report using {acc.phone}: {e}", 'error', acc.id)

                    text_idx[0] = (text_idx[0] + reports_per_account) % len(texts_list)
                    results.append(result)
                    task_progress[task_id]['completed'] += 1
                    task_progress[task_id]['results'] = results

                loop.close()
                task_progress[task_id]['status'] = 'completed'
                task_progress[task_id]['current_account'] = ''

        threading.Thread(target=run_report_sequential, daemon=True).start()
        return jsonify({'success': True, 'message': f'Reporting started for {len(valid_accounts)} account(s)', 'task_id': task_id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/report_task_status/<task_id>')
@login_required
def report_task_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── AUTO JOIN ───────────────────────────────────────────────────────────────

@app.route('/bot/auto_join', methods=['POST'])
@login_required
def auto_join():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        invite_links = [l.strip() for l in data.get('invite_links', []) if l.strip()]

        if not invite_links:
            return jsonify({'success': False, 'error': 'Enter invite links'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _join():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            joined = 0
            failed = []
            for link in invite_links:
                try:
                    if 't.me/joinchat/' in link or 't.me/+' in link:
                        hash_part = link.split('/')[-1].lstrip('+')
                        await client(functions.messages.ImportChatInviteRequest(hash=hash_part))
                    else:
                        username = link.split('/')[-1].lstrip('@')
                        entity = await client.get_entity(username)
                        await client(functions.channels.JoinChannelRequest(channel=entity))
                    joined += 1
                    await asyncio.sleep(2)
                except UserAlreadyParticipantError:
                    joined += 1
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    failed.append({'link': link, 'error': str(e)})
            await client.disconnect()
            return joined, failed

        joined, failed = run_async(_join())
        account.last_used = datetime.utcnow()
        db.session.commit()

        log_activity(current_user.id, f"Auto-joined {joined} groups/channels ({len(failed)} failed)", 'success', account_id)
        return jsonify({'success': True, 'joined': joined, 'failed': failed})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── MEMBER SCRAPER ──────────────────────────────────────────────────────────

@app.route('/bot/scrape_members', methods=['POST'])
@login_required
def scrape_members():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        group_link = data.get('group_link', '').strip()
        limit = safe_int(data.get('limit'), 100, minimum=1, maximum=MAX_SCRAPE_LIMIT)
        active_only = data.get('active_only', False)

        if not group_link:
            return jsonify({'success': False, 'error': 'Group link required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(account.user_id, account_id)

        if is_protected_target(group_link):
            return jsonify({'success': False, 'error': 'This target is protected and cannot be used with this tool.'})

        async def _scrape():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            try:
                entity = await client.get_entity(group_link)
                members = []
                cutoff_24h = datetime.utcnow() - timedelta(hours=24)
                async for user in client.iter_participants(entity, limit=limit):
                    if user.bot:
                        continue
                    if not user.username:
                        continue
                    status = getattr(user, 'status', None)
                    is_online    = isinstance(status, types.UserStatusOnline)
                    is_recently  = isinstance(status, types.UserStatusRecently)
                    # offline but was_online within last 24 hours
                    is_24h_active = (
                        isinstance(status, types.UserStatusOffline)
                        and getattr(status, 'was_online', None) is not None
                        and status.was_online.replace(tzinfo=None) >= cutoff_24h
                    )
                    if active_only and not (is_online or is_recently or is_24h_active):
                        continue
                    members.append({
                        'id': user.id,
                        'username': user.username,
                        'first_name': user.first_name or '',
                        'last_name': user.last_name or '',
                        'online': is_online,
                        'recently': is_recently,
                        'active_24h': is_24h_active,
                        'phone': user.phone
                    })
                await client.disconnect()
                return members
            except Exception as e:
                await client.disconnect()
                raise e

        members = run_async(_scrape())
        account.members_scraped += len(members)
        account.last_used = datetime.utcnow()
        db.session.commit()

        log_activity(current_user.id, f"Scraped {len(members)} members from {group_link}", 'success', account_id)
        return jsonify({'success': True, 'members': members, 'count': len(members)})

    except Exception as e:
        logger.error(f"scrape_members error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── ADD MEMBERS ─────────────────────────────────────────────────────────────

@app.route('/bot/add_members', methods=['POST'])
@login_required
def add_members():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        target_group = data.get('target_group', '').strip()
        members = data.get('members', [])
        delay = max(5, safe_int(data.get('delay'), 5))
        daily_limit = safe_int(data.get('daily_limit'), 50, minimum=1, maximum=MAX_JOIN_DAILY_LIMIT)

        if not target_group or not members:
            return jsonify({'success': False, 'error': 'Target group and members required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        user_id = current_user.id
        sess = account.session_string

        def run_add():
            with app.app_context():
                proxy = get_proxy_for_account(user_id, account_id)
                async def _add():
                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    entity = await client.get_entity(target_group)
                    is_channel = hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast')
                    added = 0
                    failed = []
                    for member in members[:daily_limit]:
                        try:
                            username = member.get('username') if isinstance(member, dict) else member
                            member_id = member.get('id') if isinstance(member, dict) else None
                            if not username and not member_id:
                                continue
                            if username:
                                username = str(username).lstrip('@')
                                user_ent = await client.get_entity(username)
                            else:
                                user_ent = await client.get_entity(int(member_id))
                            if is_channel:
                                await client(InviteToChannelRequest(channel=entity, users=[user_ent]))
                            else:
                                # Regular group: use AddChatUser
                                from telethon.tl.functions.messages import AddChatUserRequest as AddChatUserReq
                                await client(AddChatUserReq(chat_id=entity.id, user_id=user_ent, fwd_limit=0))
                            added += 1
                            await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            failed.append(str(e))
                    await client.disconnect()
                    return added, failed

                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    added, failed = loop.run_until_complete(_add())
                    loop.close()
                    acc = TelegramAccount.query.get(account_id)
                    if acc:
                        acc.members_added += added
                        acc.last_used = datetime.utcnow()
                        db.session.commit()
                    log_activity(user_id, f"Added {added} members to {target_group} ({len(failed)} failed)", 'success', account_id)
                except Exception as e:
                    logger.error(f"add_members thread error: {e}")

        threading.Thread(target=run_add, daemon=True).start()
        return jsonify({'success': True, 'message': f'Started adding {min(len(members), daily_limit)} members. Running in background.'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── BULK DM ─────────────────────────────────────────────────────────────────

@app.route('/bot/send_dm', methods=['POST'])
@login_required
def send_dm():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        # Deduplicate so a repeated username is never messaged twice.
        usernames = dedupe_preserve([
            clean_str(u).lstrip('@') for u in (data.get('usernames') or []) if clean_str(u)
        ])
        message = clean_str(data.get('message'))
        media_path = clean_str(data.get('media_path'))
        delay = safe_int(data.get('delay'), 5, minimum=3, maximum=MAX_DELAY_SECONDS)

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not usernames or (not message and not media_path):
            return jsonify({'success': False, 'error': 'Usernames and a message (or photo/video) required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'}), 400
        if account_busy(account_id):
            return jsonify({'success': False, 'error': 'Another campaign is already using this account.'}), 409

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        user_id = current_user.id
        sess = account.session_string
        task_key = task_run_start(user_id, 'bulk_dm', len(usernames))
        lock = get_account_lock(account_id)

        def run_dm():
            with lock:
                with app.app_context():
                    proxy = get_proxy_for_account(user_id, account_id)
                    result = {'sent': 0, 'failed': 0, 'skipped': 0, 'errors': []}

                    async def _dm():
                        client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                        await client.connect()
                        try:
                            for uname in usernames:
                                attempt = 0
                                while True:
                                    try:
                                        await _deliver_media_or_text(client, uname, message, media_path)
                                        result['sent'] += 1
                                        task_run_update(task_key, sent=result['sent'])
                                        await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                                        break
                                    except FloodWaitError as e:
                                        attempt += 1
                                        wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                                        await asyncio.sleep(wait)
                                        if attempt >= 2:
                                            result['skipped'] += 1
                                            note = f'{uname}: rate limit — skipped after waiting {wait}s'
                                            result['errors'].append(note)
                                            task_run_update(task_key, skipped=result['skipped'], errors=[note])
                                            break
                                    except Exception as e:
                                        result['failed'] += 1
                                        note = friendly_telegram_error(e, uname)
                                        result['errors'].append(note)
                                        task_run_update(task_key, failed=result['failed'], errors=[note])
                                        break
                        finally:
                            await client.disconnect()
                        return result

                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            result = loop.run_until_complete(_dm())
                        finally:
                            loop.close()
                        final_status = 'completed' if result['sent'] and not result['failed'] else (
                            'failed' if not result['sent'] else 'partial'
                        )
                        acc = TelegramAccount.query.get(account_id)
                        if acc:
                            acc.messages_sent_today += result['sent']
                            acc.total_messages += result['sent']
                            acc.last_used = datetime.utcnow()
                            db.session.commit()
                        task_run_finish(task_key, final_status, sent=result['sent'],
                                        failed=result['failed'], skipped=result['skipped'])
                        log_activity(
                            user_id,
                            f"Bulk DM finished: {result['sent']} sent, {result['failed']} failed, {result['skipped']} skipped",
                            'success' if result['sent'] else 'failed', account_id,
                        )
                    except Exception as e:
                        task_run_finish(task_key, 'failed', errors=[friendly_telegram_error(e)])
                        logger.error('send_dm thread error: %s', e)

        threading.Thread(target=run_dm, daemon=True).start()
        return jsonify({
            'success': True,
            'message': f'Started sending DMs to {len(usernames)} users. Running in background.',
            'task_key': task_key,
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/send_dm_multi', methods=['POST'])
@login_required
def send_dm_multi():
    """Multi-account DM: each account sends to a different slice of the usernames list."""
    try:
        data = request.get_json()
        account_ids = dedupe_preserve([safe_int(x, 0) for x in (data.get('account_ids') or []) if safe_int(x, 0)])
        usernames   = dedupe_preserve([clean_str(u).lstrip('@') for u in (data.get('usernames') or []) if clean_str(u)])
        message     = clean_str(data.get('message'))
        media_path  = clean_str(data.get('media_path'))
        delay       = safe_int(data.get('delay'), 5, minimum=3, maximum=MAX_DELAY_SECONDS)

        if not account_ids or not usernames or (not message and not media_path):
            return jsonify({'success': False, 'error': 'Accounts, usernames, and a message (or photo/video) required'}), 400

        # Validate every account up front: the UI must not be told "started"
        # while an unowned/disconnected account silently does nothing.
        valid_accounts, rejected = [], []
        for acc_id in account_ids:
            acc = get_account_for_current_user(acc_id)
            if not acc:
                rejected.append(f'{acc_id}: not found or not yours')
                continue
            if not acc.session_string or acc.session_string in ('demo', 'active'):
                rejected.append(f'{acc.phone}: not connected')
                continue
            if account_busy(acc_id):
                rejected.append(f'{acc.phone}: busy with another campaign')
                continue
            valid_accounts.append(acc)
        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No usable account selected. ' + '; '.join(rejected[:3])}), 400

        n = len(valid_accounts)
        # Even round-robin distribution — every account gets work, no empty slices.
        slices = [[] for _ in range(n)]
        for index, name in enumerate(usernames):
            slices[index % n].append(name)

        user_id = current_user.id
        task_key = task_run_start(user_id, 'bulk_dm_multi', len(usernames))

        def _run_slice(acc_id, names, phone):
            with get_account_lock(acc_id):
                with app.app_context():
                    acc = TelegramAccount.query.get(acc_id)
                    if not acc or not acc.session_string:
                        return
                    ai, ah = get_api_credentials(acc.user_id)
                    if not ai:
                        task_run_update(task_key, errors=[f'{phone}: API credentials missing'])
                        return
                    proxy = get_proxy_for_account(acc.user_id, acc_id)
                    result = {'sent': 0, 'failed': 0, 'skipped': 0}

                    async def _dm():
                        client = TelegramClient(StringSession(acc.session_string), ai, ah, proxy=proxy)
                        await client.connect()
                        try:
                            for uname in names:
                                attempt = 0
                                while True:
                                    try:
                                        await _deliver_media_or_text(client, uname, message, media_path)
                                        result['sent'] += 1
                                        await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                                        break
                                    except FloodWaitError as e:
                                        attempt += 1
                                        wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                                        await asyncio.sleep(wait)
                                        if attempt >= 2:
                                            result['skipped'] += 1
                                            break
                                    except Exception as e:
                                        result['failed'] += 1
                                        task_run_update(task_key, errors=[friendly_telegram_error(e, uname)])
                                        break
                        finally:
                            await client.disconnect()
                        return result

                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(_dm())
                        a = TelegramAccount.query.get(acc_id)
                        if a:
                            a.messages_sent_today += result['sent']
                            a.total_messages += result['sent']
                            a.last_used = datetime.utcnow()
                            db.session.commit()
                        snapshot = task_run_snapshot(task_key) or {}
                        task_run_update(
                            task_key,
                            sent=safe_int(snapshot.get('sent'), 0) + result['sent'],
                            failed=safe_int(snapshot.get('failed'), 0) + result['failed'],
                            skipped=safe_int(snapshot.get('skipped'), 0) + result['skipped'],
                        )
                        log_activity(
                            user_id, f"Multi-DM {phone}: {result['sent']} sent, {result['failed']} failed",
                            'success' if result['sent'] else 'failed', acc_id,
                        )
                    except Exception as e:
                        task_run_update(task_key, errors=[friendly_telegram_error(e, phone)])
                        logger.error('send_dm_multi error %s: %s', acc_id, e)
                    finally:
                        loop.close()

        threads = []
        for acc, names in zip(valid_accounts, slices):
            if names:
                thread = threading.Thread(target=_run_slice, args=(acc.id, names, acc.phone), daemon=True)
                thread.start()
                threads.append(thread)

        def _watch_finish(threads=threads):
            for thread in threads:
                thread.join()
            snapshot = task_run_snapshot(task_key) or {}
            sent = safe_int(snapshot.get('sent'), 0)
            failed = safe_int(snapshot.get('failed'), 0)
            final_status = 'completed' if sent and not failed else ('failed' if not sent else 'partial')
            task_run_finish(task_key, final_status)
            log_activity(
                user_id, f"Multi-DM finished: {sent} sent, {failed} failed",
                'success' if sent else 'failed',
            )

        threading.Thread(target=_watch_finish, daemon=True).start()

        return jsonify({
            'success': True,
            'message': f'Started: {len(valid_accounts)} account(s) sending to {len(usernames)} users.',
            'task_key': task_key,
            'rejected': rejected,
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── DM CAMPAIGN (Targeted) ──────────────────────────────────────────────────

@app.route('/bot/dm_campaign', methods=['POST'])
@login_required
def dm_campaign():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        targets = data.get('targets') or []
        messages = [clean_str(m) for m in (data.get('messages') or []) if clean_str(m)]
        delay = safe_int(data.get('delay'), 5, minimum=3, maximum=MAX_DELAY_SECONDS)
        personalize = safe_bool(data.get('personalize'), False)
        media_path = clean_str(data.get('media_path'))

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not targets:
            return jsonify({'success': False, 'error': 'No targets provided'}), 400
        if not messages and not media_path:
            return jsonify({'success': False, 'error': 'No messages provided'}), 400
        if media_path and not messages:
            # Media-only campaign: the caption is empty, no template needed.
            messages = ['']

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'}), 400
        if account_busy(account_id):
            return jsonify({'success': False, 'error': 'Another campaign is already using this account.'}), 409

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        # Deduplicate targets so nobody receives the campaign twice.
        def _target_key(target):
            if isinstance(target, dict):
                return clean_str(target.get('username') or target.get('id')).lstrip('@').lower()
            return clean_str(target).lstrip('@').lower()

        targets = dedupe_preserve([t for t in targets if _target_key(t)], key=_target_key)

        task_id = uuid.uuid4().hex
        task_run_start(current_user.id, 'dm_campaign', len(targets), task_key=task_id)
        user_id = current_user.id
        sess = account.session_string
        lock = get_account_lock(account_id)

        def run_campaign():
            with lock:
                with app.app_context():
                    proxy = get_proxy_for_account(user_id, account_id)
                    result = {'sent': 0, 'failed': 0, 'skipped': 0, 'errors': []}

                    async def _campaign():
                        client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                        await client.connect()
                        try:
                            for i, target in enumerate(targets):
                                username = clean_str(
                                    target.get('username') if isinstance(target, dict) else target
                                ).lstrip('@')
                                if not username:
                                    result['skipped'] += 1
                                    continue
                                msg = messages[i % len(messages)]
                                if personalize and isinstance(target, dict):
                                    first_name = clean_str(target.get('first_name'), 'there') or 'there'
                                    msg = msg.replace('{name}', first_name).replace('{first_name}', first_name)
                                attempt = 0
                                while True:
                                    try:
                                        await _deliver_media_or_text(client, username, msg, media_path)
                                        result['sent'] += 1
                                        task_run_update(task_id, sent=result['sent'], skipped=result['skipped'])
                                        await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                                        break
                                    except FloodWaitError as e:
                                        attempt += 1
                                        wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                                        await asyncio.sleep(wait)
                                        if attempt >= 2:
                                            result['skipped'] += 1
                                            note = f'{username}: rate limit — skipped after waiting {wait}s'
                                            result['errors'].append(note)
                                            task_run_update(task_id, skipped=result['skipped'], errors=[note])
                                            break
                                    except Exception as e:
                                        result['failed'] += 1
                                        note = friendly_telegram_error(e, username)
                                        result['errors'].append(note)
                                        task_run_update(task_id, failed=result['failed'], errors=[note])
                                        break
                        finally:
                            await client.disconnect()
                        return result

                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            result = loop.run_until_complete(_campaign())
                        finally:
                            loop.close()
                        final_status = 'completed' if result['sent'] and not result['failed'] else (
                            'failed' if not result['sent'] else 'partial'
                        )
                        acc = TelegramAccount.query.get(account_id)
                        if acc:
                            acc.messages_sent_today += result['sent']
                            acc.total_messages += result['sent']
                            acc.last_used = datetime.utcnow()
                            db.session.commit()
                        task_run_finish(
                            task_id, final_status, sent=result['sent'],
                            failed=result['failed'], skipped=result['skipped'],
                        )
                        log_activity(
                            user_id,
                            f"DM Campaign finished: {result['sent']} sent, {result['failed']} failed, {result['skipped']} skipped",
                            'success' if result['sent'] else 'failed', account_id,
                        )
                    except Exception as e:
                        task_run_finish(task_id, 'failed', errors=[friendly_telegram_error(e)])
                        logger.error('dm_campaign thread error: %s', e)

        threading.Thread(target=run_campaign, daemon=True).start()
        return jsonify({'success': True, 'message': f'Campaign started for {len(targets)} targets', 'task_id': task_id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/campaign_status/<task_id>')
@login_required
def campaign_status(task_id):
    # Reads the durable TaskRun row, so progress is still available after a restart.
    progress = task_run_snapshot(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found (it may have finished before the last restart)'})
    return jsonify({'success': True, 'progress': progress})


# ─── SUBSCRIPTION ────────────────────────────────────────────────────────────

@app.route('/bot/subscription_info')
@login_required
def subscription_info():
    sub = get_user_subscription(current_user.id)
    plan = sub['plan']
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])
    account_count = TelegramAccount.query.filter_by(user_id=current_user.id).count()
    return jsonify({
        'success': True,
        'plan': plan,
        'label': limits['label'],
        'color': limits['color'],
        'price': limits['price'],
        'expires_at': sub['expires_at'].strftime('%Y-%m-%d') if sub['expires_at'] else None,
        'expired': sub['expired'],
        'limits': limits,
        'account_count': account_count,
        'is_admin': current_user.username in ADMIN_USERS,
        'plans': PLAN_LIMITS,
    })


@app.route('/bot/redeem_key', methods=['POST'])
@login_required
def redeem_key():
    try:
        data = request.get_json()
        key_str = (data.get('key') or '').strip().upper()
        if not key_str:
            return jsonify({'success': False, 'error': 'Enter a license key'})

        lk = LicenseKey.query.filter_by(key=key_str).first()
        if not lk:
            return jsonify({'success': False, 'error': 'Invalid license key'})
        if not lk.is_active:
            return jsonify({'success': False, 'error': 'This key has been deactivated'})
        if lk.use_count >= lk.max_uses:
            return jsonify({'success': False, 'error': 'This key has already been used'})

        sub = Subscription.query.filter_by(user_id=current_user.id).first()
        now = datetime.utcnow()
        if not sub:
            sub = Subscription(user_id=current_user.id)
            db.session.add(sub)

        sub.plan = lk.plan
        if lk.duration_days > 0:
            current_expiry = sub.expires_at if (sub.expires_at and sub.expires_at > now) else now
            sub.expires_at = current_expiry + timedelta(days=lk.duration_days)
        else:
            sub.expires_at = None  # unlimited
        sub.updated_at = now

        lk.use_count += 1
        lk.used_by_id = current_user.id
        lk.used_at = now
        if lk.use_count >= lk.max_uses:
            lk.is_active = False

        db.session.commit()
        exp_str = sub.expires_at.strftime('%Y-%m-%d') if sub.expires_at else 'Never'
        return jsonify({'success': True, 'message': f'✅ {lk.plan.capitalize()} plan activated! Expires: {exp_str}'})
    except Exception as e:
        logger.error(f"redeem_key error: {e}")
        return jsonify({'success': False, 'error': str(e)})


# ─── ADMIN ────────────────────────────────────────────────────────────────────

def admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or current_user.username not in ADMIN_USERS:
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated


@app.route('/admin')
@login_required
def admin_panel():
    if current_user.username not in ADMIN_USERS:
        return redirect(url_for('dashboard'))
    return render_template('admin.html', user=current_user)


@app.route('/bot/admin/users')
@login_required
@admin_required
def admin_users():
    users = User.query.order_by(User.created_at.desc()).all()
    result = []
    for u in users:
        sub = get_user_subscription(u.id)
        ac_count = TelegramAccount.query.filter_by(user_id=u.id).count()
        result.append({
            'id': u.id,
            'username': u.username,
            'created_at': u.created_at.strftime('%Y-%m-%d %H:%M'),
            'plan': sub['plan'],
            'expires_at': sub['expires_at'].strftime('%Y-%m-%d') if sub['expires_at'] else '–',
            'expired': sub['expired'],
            'accounts': ac_count,
            'is_banned': bool(getattr(u, 'is_banned', False)),
            'last_login_at': u.last_login_at.strftime('%Y-%m-%d %H:%M') if getattr(u, 'last_login_at', None) else 'never',
        })
    return jsonify({'success': True, 'users': result})


@app.route('/bot/admin/set_plan', methods=['POST'])
@login_required
@admin_required
def admin_set_plan():
    try:
        data = request.get_json()
        user_id = safe_int(data.get('user_id'), 0)
        plan = data.get('plan', 'free')
        days = safe_int(data.get('days'), 30)

        if plan not in PLAN_LIMITS:
            return jsonify({'success': False, 'error': 'Invalid plan'})

        sub = Subscription.query.filter_by(user_id=user_id).first()
        if not sub:
            sub = Subscription(user_id=user_id)
            db.session.add(sub)

        now = datetime.utcnow()
        sub.plan = plan
        sub.updated_at = now
        if plan == 'unlimited' or days == 0:
            sub.expires_at = None
        else:
            sub.expires_at = now + timedelta(days=days)

        db.session.commit()
        return jsonify({'success': True, 'message': f'Plan set to {plan}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/admin/generate_keys', methods=['POST'])
@login_required
@admin_required
def admin_generate_keys():
    try:
        data = request.get_json()
        count = safe_int(data.get('count'), 1, minimum=1, maximum=MAX_BULK_COUNT)
        plan = data.get('plan', 'basic')
        days = safe_int(data.get('days'), 30)
        max_uses = safe_int(data.get('max_uses'), 1)
        notes = data.get('notes', '')

        if plan not in PLAN_LIMITS:
            return jsonify({'success': False, 'error': 'Invalid plan'})

        keys = []
        for _ in range(count):
            while True:
                key = generate_license_key()
                if not LicenseKey.query.filter_by(key=key).first():
                    break
            lk = LicenseKey(
                key=key, plan=plan, duration_days=days,
                max_uses=max_uses, notes=notes
            )
            db.session.add(lk)
            keys.append(key)

        db.session.commit()
        return jsonify({'success': True, 'keys': keys, 'count': len(keys)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/admin/keys')
@login_required
@admin_required
def admin_keys():
    keys = LicenseKey.query.order_by(LicenseKey.created_at.desc()).limit(MAX_LIST_ROWS).all()
    result = []
    for k in keys:
        used_by_name = None
        if k.used_by_id:
            u = User.query.get(k.used_by_id)
            used_by_name = u.username if u else f'User#{k.used_by_id}'
        result.append({
            'id': k.id,
            'key': k.key,
            'plan': k.plan,
            'duration_days': k.duration_days,
            'max_uses': k.max_uses,
            'use_count': k.use_count,
            'is_active': k.is_active,
            'used_by': used_by_name,
            'used_at': k.used_at.strftime('%Y-%m-%d') if k.used_at else None,
            'created_at': k.created_at.strftime('%Y-%m-%d'),
            'notes': k.notes or '',
        })
    return jsonify({'success': True, 'keys': result})


@app.route('/bot/admin/revoke_key/<int:key_id>', methods=['POST'])
@login_required
@admin_required
def admin_revoke_key(key_id):
    lk = LicenseKey.query.get_or_404(key_id)
    lk.is_active = not lk.is_active
    db.session.commit()
    return jsonify({'success': True, 'active': lk.is_active})


@app.route('/bot/admin/delete_user/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_delete_user(user_id):
    if user_id == current_user.id:
        return jsonify({'success': False, 'error': 'Cannot delete yourself'})
    user = User.query.get_or_404(user_id)
    db.session.delete(user)
    db.session.commit()
    return jsonify({'success': True})


@app.route('/bot/admin/stats')
@login_required
@admin_required
def admin_stats():
    total_users = User.query.count()
    total_accounts = TelegramAccount.query.count()
    total_keys = LicenseKey.query.count()
    used_keys = LicenseKey.query.filter(LicenseKey.use_count > 0).count()
    plan_counts = {}
    for plan in PLAN_LIMITS:
        subs = Subscription.query.filter_by(plan=plan).count()
        plan_counts[plan] = subs
    plan_counts['free'] = total_users - sum(v for k, v in plan_counts.items() if k != 'free')
    return jsonify({
        'success': True,
        'total_users': total_users,
        'total_accounts': total_accounts,
        'total_keys': total_keys,
        'used_keys': used_keys,
        'plan_counts': plan_counts,
    })


# ─── NEW TELEGRAM FEATURES ────────────────────────────────────────────────────

@app.route('/bot/check_username', methods=['POST'])
@login_required
def check_username():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        username = (data.get('username') or '').strip().lstrip('@')
        if not username:
            return jsonify({'success': False, 'error': 'Enter a username'})

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _check():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                result = await client(functions.account.CheckUsernameRequest(username=username))
                return result
            finally:
                await client.disconnect()

        available = run_async(_check())
        return jsonify({'success': True, 'username': username, 'available': bool(available),
                        'message': f'@{username} is {"✅ available!" if available else "❌ taken"}'})
    except Exception as e:
        logger.error(f"check_username error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/send_saved_message', methods=['POST'])
@login_required
def send_saved_message():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        message = (data.get('message') or '').strip()
        if not message:
            return jsonify({'success': False, 'error': 'Message required'})

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _send():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                await client.send_message('me', message)
            finally:
                await client.disconnect()

        run_async(_send())
        log_activity(current_user.id, f"Sent message to Saved Messages", 'success', account_id)
        return jsonify({'success': True, 'message': 'Sent to Saved Messages ✅'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/delete_my_messages', methods=['POST'])
@login_required
def delete_my_messages():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        chat = (data.get('chat') or '').strip()
        count = safe_int(data.get('count'), 10, minimum=1, maximum=MAX_BULK_COUNT)
        if not chat:
            return jsonify({'success': False, 'error': 'Chat/group required'})

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _delete():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                me = await client.get_me()
                entity = await client.get_entity(chat)
                msg_ids = []
                async for msg in client.iter_messages(entity, limit=count * 3, from_user=me.id):
                    msg_ids.append(msg.id)
                    if len(msg_ids) >= count:
                        break
                if msg_ids:
                    await client.delete_messages(entity, msg_ids)
                return len(msg_ids)
            finally:
                await client.disconnect()

        deleted = run_async(_delete())
        log_activity(current_user.id, f"Deleted {deleted} messages from {chat}", 'success', account_id)
        return jsonify({'success': True, 'message': f'Deleted {deleted} messages ✅'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/get_group_info', methods=['POST'])
@login_required
def get_group_info():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        group = (data.get('group') or '').strip()
        if not group:
            return jsonify({'success': False, 'error': 'Group/channel required'})

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})

        api_id, api_hash_val = get_api_credentials(current_user.id)
        proxy = get_proxy_for_account(current_user.id, account_id)

        async def _info():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash_val, proxy=proxy)
            await client.connect()
            try:
                entity = await client.get_entity(group)
                full = await client.get_participants(entity, limit=0)
                title = getattr(entity, 'title', str(entity))
                username = getattr(entity, 'username', None)
                member_count = getattr(entity, 'participants_count', None) or full.total
                entity_id = entity.id
                is_channel = hasattr(entity, 'broadcast') and entity.broadcast
                is_megagroup = getattr(entity, 'megagroup', False)
                restricted = getattr(entity, 'restricted', False)
                verified = getattr(entity, 'verified', False)
                scam = getattr(entity, 'scam', False)
                return {
                    'title': title,
                    'username': username,
                    'id': entity_id,
                    'members': member_count,
                    'type': 'Channel' if is_channel else ('Supergroup' if is_megagroup else 'Group'),
                    'restricted': restricted,
                    'verified': verified,
                    'scam': scam,
                }
            finally:
                await client.disconnect()

        info = run_async(_info())
        return jsonify({'success': True, 'info': info})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ─── DASHBOARD STATS ─────────────────────────────────────────────────────────

@app.route('/bot/dashboard_stats')
@login_required
def dashboard_stats():
    try:
        admin = is_admin_user()
        # Admin sees ALL accounts across all users; regular user sees only their own
        accounts = get_accounts_for_current_user()
        logs = (ActivityLog.query.order_by(ActivityLog.timestamp.desc()).limit(LOG_ROWS_LIMIT).all()
                if admin else
                ActivityLog.query.filter_by(user_id=current_user.id).order_by(ActivityLog.timestamp.desc()).limit(LOG_ROWS_LIMIT).all())
        scheduled = (ScheduledBroadcast.query.filter_by(status='pending').count()
                     if admin else
                     ScheduledBroadcast.query.filter_by(user_id=current_user.id, status='pending').count())

        sub = get_user_subscription(current_user.id)
        plan = sub['plan']
        limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])

        # Build health map (all accounts when admin)
        health_q = AccountHealth.query.all() if admin else AccountHealth.query.filter_by(user_id=current_user.id).all()
        health_map = {h.account_id: h for h in health_q}

        # Build owner username map for admin view
        owner_map = {}
        if admin:
            users = User.query.all()
            owner_map = {u.id: u.username for u in users}

        return jsonify({
            # `success` added for a consistent response contract across all APIs
            'success': True,
            'total_accounts': len(accounts),
            'messages_today': sum(a.messages_sent_today for a in accounts),
            'reports_today': sum(a.reports_sent_today for a in accounts),
            'members_scraped': sum(a.members_scraped for a in accounts),
            'members_added': sum(a.members_added for a in accounts),
            'scheduled_broadcasts': scheduled,
            'plan': plan,
            'plan_label': limits['label'],
            'plan_color': limits['color'],
            'plan_limit_accounts': limits['accounts'],
            'is_admin': admin,
            'accounts': [{
                'id': a.id,
                'phone': a.phone,
                'is_active': a.is_active,
                'connected': bool(a.session_string and a.session_string not in ('demo', 'active')),
                'messages_sent': a.messages_sent_today,
                'reports_sent': a.reports_sent_today,
                'members_scraped': a.members_scraped,
                'members_added': a.members_added,
                'total_messages': a.total_messages,
                'total_reports': a.total_reports,
                'owner': owner_map.get(a.user_id, '') if admin else '',
                'health_status':    health_map[a.id].status if a.id in health_map else 'unchecked',
                'health_detail':    health_map[a.id].detail if a.id in health_map else '',
                'health_checked':   health_map[a.id].checked_at.isoformat() if a.id in health_map else None,
                'tg_name':          getattr(health_map.get(a.id), 'tg_name', '') or '',
                'tg_username':      getattr(health_map.get(a.id), 'tg_username', '') or '',
                'spambot_released': getattr(health_map.get(a.id), 'spambot_released', False),
                'spambot_result':   getattr(health_map.get(a.id), 'spambot_result', '') or ''
            } for a in accounts],
            'logs': [{
                'action': log.action,
                'status': log.status,
                'timestamp': log.timestamp.isoformat()
            } for log in logs]
        })
    except Exception as e:
        logger.error('dashboard_stats error: %s', e)
        # Same response contract as every other API route.
        return jsonify({'success': False, 'error': 'Could not load dashboard statistics — see server logs.'}), 500


@app.route('/bot/reset_daily_stats', methods=['POST'])
@login_required
def reset_daily_stats():
    try:
        accounts = TelegramAccount.query.filter_by(user_id=current_user.id).all()
        for a in accounts:
            a.messages_sent_today = 0
            a.reports_sent_today = 0
        db.session.commit()
        log_activity(current_user.id, "Reset daily statistics", 'success')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── BACKGROUND THREADS ──────────────────────────────────────────────────────

def schedule_daily_reset():
    def reset():
        while True:
            now = datetime.now()
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time.sleep((next_midnight - now).total_seconds())
            with app.app_context():
                for a in TelegramAccount.query.all():
                    a.messages_sent_today = 0
                    a.reports_sent_today = 0
                db.session.commit()
    threading.Thread(target=reset, daemon=True).start()


def run_scheduled_broadcasts():
    def checker():
        while True:
            time.sleep(30)
            try:
                with app.app_context():
                    now = datetime.utcnow()
                    pending = ScheduledBroadcast.query.filter(
                        ScheduledBroadcast.status == 'pending',
                        ScheduledBroadcast.scheduled_at <= now
                    ).all()

                    for sb in pending:
                        sb.status = 'running'
                        db.session.commit()
                        sb_id = sb.id
                        account_id = sb.account_id
                        user_id = sb.user_id

                        acc = TelegramAccount.query.get(account_id)
                        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
                            sb.status = 'failed'
                            db.session.commit()
                            continue

                        api_id, api_hash = get_api_credentials(user_id)
                        if not api_id:
                            sb.status = 'failed'
                            db.session.commit()
                            continue

                        try:
                            group_ids = dedupe_preserve(json.loads(sb.groups or '[]'))
                        except Exception:
                            group_ids = []
                        if not group_ids:
                            sb.status = 'failed'
                            db.session.commit()
                            log_activity(user_id, f"Scheduled broadcast {sb_id} has no valid targets", 'failed', account_id)
                            continue
                        content = sb.content
                        caption = sb.caption or ''
                        message_type = sb.message_type
                        delay = safe_int(sb.delay, 3, minimum=1)
                        sess = acc.session_string

                        repeat_interval = safe_int(getattr(sb, 'repeat_interval', 0), 0, minimum=0)
                        repeat_count    = safe_int(getattr(sb, 'repeat_count', 0), 0, minimum=0)
                        task_key = f'sched:{sb_id}'
                        task_run_start(user_id, 'scheduled_broadcast', len(group_ids), task_key=task_key)

                        def run_sb(sb_id=sb_id, account_id=account_id, user_id=user_id,
                                   group_ids=group_ids, content=content, caption=caption,
                                   message_type=message_type, delay=delay, sess=sess,
                                   api_id=api_id, api_hash=api_hash, task_key=task_key,
                                   repeat_interval=repeat_interval, repeat_count=repeat_count):
                            with app.app_context():
                                proxy = get_proxy_for_account(user_id, account_id)
                                result = {'sent': 0, 'failed': 0, 'skipped': 0, 'errors': []}

                                async def _sb():
                                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                                    await client.connect()
                                    try:
                                        for gid in group_ids:
                                            attempt = 0
                                            while True:
                                                try:
                                                    entity = await client.get_entity(safe_int(gid, 0) or gid)
                                                    if message_type == 'text':
                                                        await client.send_message(entity, content)
                                                    else:
                                                        await client.send_file(entity, content, caption=caption or content)
                                                    result['sent'] += 1
                                                    task_run_update(task_key, sent=result['sent'])
                                                    await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                                                    break
                                                except FloodWaitError as e:
                                                    attempt += 1
                                                    wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                                                    await asyncio.sleep(wait)
                                                    if attempt >= 2:
                                                        result['skipped'] += 1
                                                        note = f'{gid}: rate limit — skipped after waiting {wait}s'
                                                        result['errors'].append(note)
                                                        task_run_update(task_key, skipped=result['skipped'], errors=[note])
                                                        break
                                                except Exception as e:
                                                    result['failed'] += 1
                                                    note = friendly_telegram_error(e, str(gid))
                                                    result['errors'].append(note)
                                                    task_run_update(task_key, failed=result['failed'], errors=[note])
                                                    break
                                    finally:
                                        await client.disconnect()
                                    return result

                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                try:
                                    result = loop.run_until_complete(_sb())
                                    # zero confirmed sends is never reported as success
                                    final_status = 'completed' if result['sent'] and not result['failed'] else (
                                        'failed' if not result['sent'] else 'partial'
                                    )
                                    s = ScheduledBroadcast.query.get(sb_id)
                                    acc2 = TelegramAccount.query.get(account_id)
                                    if s:
                                        s.status = final_status
                                        s.sent_count = result['sent']
                                        s.completed_at = datetime.utcnow()
                                    if acc2:
                                        acc2.messages_sent_today += result['sent']
                                        acc2.total_messages += result['sent']
                                    db.session.commit()
                                    task_run_finish(
                                        task_key, final_status, sent=result['sent'],
                                        failed=result['failed'], skipped=result['skipped'],
                                    )
                                    log_activity(
                                        user_id,
                                        f"Scheduled broadcast: {result['sent']} sent, {result['failed']} failed, "
                                        f"{result['skipped']} skipped",
                                        'success' if result['sent'] else 'failed', account_id,
                                    )
                                    # Repeat semantics (matches the UI label "0 = ∞"):
                                    #   repeat_count == 0  -> repeat forever
                                    #   repeat_count == N  -> N runs in total
                                    if s:
                                        should_repeat, remaining = next_repeat_settings(repeat_interval, repeat_count)
                                        if should_repeat:
                                            new_sb = ScheduledBroadcast(
                                                user_id=user_id,
                                                account_id=account_id,
                                                message_type=message_type,
                                                content=content,
                                                caption=caption,
                                                groups=json.dumps(group_ids),
                                                delay=delay,
                                                scheduled_at=datetime.utcnow() + timedelta(hours=repeat_interval),
                                                status='pending',
                                                repeat_interval=repeat_interval,
                                                repeat_count=remaining
                                            )
                                            db.session.add(new_sb)
                                            db.session.commit()
                                            log_activity(
                                                user_id,
                                                f"Repeat broadcast scheduled in {repeat_interval}h"
                                                + ('' if remaining == 0 else f" ({remaining} run(s) left)"),
                                                'success', account_id,
                                            )
                                        else:
                                            log_activity(user_id, 'Repeat broadcast finished (repeat count reached)', 'success', account_id)
                                except Exception as e:
                                    s = ScheduledBroadcast.query.get(sb_id)
                                    if s:
                                        s.status = 'failed'
                                        db.session.commit()
                                    task_run_finish(task_key, 'failed', errors=[friendly_telegram_error(e)])
                                    logger.error('Scheduled broadcast failed: %s', e)
                                finally:
                                    loop.close()

                        threading.Thread(target=run_sb, daemon=True).start()
            except Exception as e:
                logger.error(f"Scheduled broadcast checker error: {e}")

    threading.Thread(target=checker, daemon=True).start()


# ─── MULTI-ACCOUNT ADD MEMBERS ───────────────────────────────────────────────

@app.route('/bot/add_members_multi', methods=['POST'])
@login_required
def add_members_multi():
    """Add members using multiple accounts concurrently with live progress tracking."""
    try:
        data = request.get_json()
        account_ids = [safe_int(x, 0) for x in (data.get('account_ids') or []) if safe_int(x, 0)]
        target_group = data.get('target_group', '').strip()
        members = data.get('members', [])
        delay = max(5, safe_int(data.get('delay'), 5))
        daily_limit_per_account = safe_int(data.get('daily_limit'), 50, minimum=1, maximum=MAX_JOIN_DAILY_LIMIT)
        distribute = data.get('distribute', True)

        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'}), 400
        if not target_group:
            return jsonify({'success': False, 'error': 'Target group required'}), 400
        if not members:
            return jsonify({'success': False, 'error': 'Member list is empty'}), 400

        valid_accounts = []
        for aid in account_ids:
            acc = TelegramAccount.query.get(aid)
            if acc and acc.user_id == current_user.id and acc.session_string and acc.session_string not in ('demo', 'active'):
                valid_accounts.append(acc)

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts selected'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        task_id = str(uuid.uuid4())
        per_account = {}
        if distribute:
            chunk = max(1, len(members) // len(valid_accounts))
            for i, acc in enumerate(valid_accounts):
                start = i * chunk
                end = start + chunk if i < len(valid_accounts) - 1 else len(members)
                per_account[acc.id] = members[start:end]
        else:
            for acc in valid_accounts:
                per_account[acc.id] = members[:daily_limit_per_account]

        task_progress[task_id] = {
            'status': 'running',
            'total_added': 0,
            'total_failed': 0,
            'total_members': len(members),
            'accounts': {str(acc.id): {'phone': acc.phone, 'added': 0, 'failed': 0, 'status': 'waiting', 'errors': []}
                         for acc in valid_accounts}
        }

        user_id = current_user.id

        def run_account(acc, m_list):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)
                task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'running'

                async def _add():
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    added = 0
                    failed = 0
                    errors = []
                    try:
                        entity = await client.get_entity(target_group)
                        for member in m_list[:daily_limit_per_account]:
                            try:
                                username = member.get('username') if isinstance(member, dict) else str(member)
                                if not username:
                                    continue
                                username = username.lstrip('@')
                                user_ent = await client.get_entity(username)
                                await client(InviteToChannelRequest(channel=entity, users=[user_ent]))
                                added += 1
                                task_progress[task_id]['accounts'][str(acc.id)]['added'] = added
                                task_progress[task_id]['total_added'] = sum(
                                    v['added'] for v in task_progress[task_id]['accounts'].values()
                                )
                                await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                            except FloodWaitError as e:
                                await asyncio.sleep(e.seconds)
                            except Exception as e:
                                failed += 1
                                errors.append(str(e)[:80])
                                task_progress[task_id]['accounts'][str(acc.id)]['failed'] = failed
                                task_progress[task_id]['total_failed'] = sum(
                                    v['failed'] for v in task_progress[task_id]['accounts'].values()
                                )
                    finally:
                        await client.disconnect()
                    return added, failed, errors

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    added, failed, errors = loop.run_until_complete(_add())
                    a = TelegramAccount.query.get(acc.id)
                    if a:
                        a.members_added += added
                        a.last_used = datetime.utcnow()
                        db.session.commit()
                    task_progress[task_id]['accounts'][str(acc.id)].update({
                        'status': 'done', 'added': added, 'failed': failed, 'errors': errors
                    })
                    log_activity(user_id, f"[Multi] Added {added} members to {target_group} via {acc.phone}", 'success', acc.id)
                except Exception as e:
                    task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'failed'
                    task_progress[task_id]['accounts'][str(acc.id)]['errors'].append(str(e))
                    logger.error(f"add_members_multi account {acc.phone} error: {e}")
                finally:
                    loop.close()

            # check if all done
            all_done = all(v['status'] in ('done', 'failed')
                           for v in task_progress[task_id]['accounts'].values())
            if all_done:
                task_progress[task_id]['status'] = 'completed'

        for acc in valid_accounts:
            threading.Thread(target=run_account, args=(acc, per_account[acc.id]), daemon=True).start()

        return jsonify({'success': True, 'task_id': task_id,
                        'message': f'Started adding members using {len(valid_accounts)} accounts'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/add_members_multi_status/<task_id>')
@login_required
def add_members_multi_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── ALL-ACCOUNTS AUTO JOIN ───────────────────────────────────────────────────

@app.route('/bot/auto_join_all', methods=['POST'])
@login_required
def auto_join_all():
    """Join groups/channels with ALL connected accounts simultaneously."""
    try:
        data = request.get_json()
        invite_links = [l.strip() for l in data.get('invite_links', []) if l.strip()]
        account_ids = data.get('account_ids', [])

        if not invite_links:
            return jsonify({'success': False, 'error': 'Enter at least one invite link'}), 400

        valid_accounts = []
        if account_ids:
            for aid in account_ids:
                acc = TelegramAccount.query.get(int(aid))
                if acc and acc.user_id == current_user.id and acc.session_string and acc.session_string not in ('demo', 'active'):
                    valid_accounts.append(acc)
        else:
            all_accs = TelegramAccount.query.filter_by(user_id=current_user.id, is_active=True).all()
            valid_accounts = [a for a in all_accs if a.session_string and a.session_string not in ('demo', 'active')]

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts available'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {
            'status': 'running',
            'total_joined': 0,
            'total_accounts': len(valid_accounts),
            'completed_accounts': 0,
            'accounts': {str(acc.id): {'phone': acc.phone, 'joined': 0, 'failed': [], 'status': 'waiting'}
                         for acc in valid_accounts}
        }

        user_id = current_user.id

        def join_with_account(acc):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)
                task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'running'

                async def _join():
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    joined = 0
                    failed = []

                    async def _join_one(link):
                        nonlocal joined
                        try:
                            if 't.me/joinchat/' in link or 't.me/+' in link:
                                hash_part = link.split('/')[-1].lstrip('+')
                                await client(functions.messages.ImportChatInviteRequest(hash=hash_part))
                            else:
                                username = link.split('/')[-1].lstrip('@')
                                entity = await client.get_entity(username)
                                await client(functions.channels.JoinChannelRequest(channel=entity))
                            joined += 1
                        except UserAlreadyParticipantError:
                            joined += 1
                        except FloodWaitError as e:
                            await asyncio.sleep(min(e.seconds, 30))
                        except Exception as e:
                            failed.append({'link': link, 'error': str(e)[:80]})

                    await asyncio.gather(*[_join_one(link) for link in invite_links])
                    await client.disconnect()
                    return joined, failed

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    joined, failed = loop.run_until_complete(_join())
                    a = TelegramAccount.query.get(acc.id)
                    if a:
                        a.last_used = datetime.utcnow()
                        db.session.commit()
                    task_progress[task_id]['accounts'][str(acc.id)].update({
                        'status': 'done', 'joined': joined, 'failed': failed
                    })
                    task_progress[task_id]['total_joined'] += joined
                    log_activity(user_id, f"[All-Join] {acc.phone} joined {joined}/{len(invite_links)} groups", 'success', acc.id)
                except Exception as e:
                    task_progress[task_id]['accounts'][str(acc.id)]['status'] = 'failed'
                    task_progress[task_id]['accounts'][str(acc.id)]['failed'].append({'error': str(e)})
                    logger.error(f"auto_join_all account {acc.phone} error: {e}")
                finally:
                    loop.close()
                    task_progress[task_id]['completed_accounts'] += 1
                    if task_progress[task_id]['completed_accounts'] >= len(valid_accounts):
                        task_progress[task_id]['status'] = 'completed'

        for acc in valid_accounts:
            threading.Thread(target=join_with_account, args=(acc,), daemon=True).start()

        return jsonify({'success': True, 'task_id': task_id,
                        'message': f'All {len(valid_accounts)} accounts are now joining the group(s)'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/auto_join_all_status/<task_id>')
@login_required
def auto_join_all_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── POST REACTIONS ───────────────────────────────────────────────────────────

@app.route('/bot/react_posts', methods=['POST'])
@login_required
def react_posts():
    try:
        data = request.get_json()
        account_ids = [safe_int(x, 0) for x in (data.get('account_ids') or []) if safe_int(x, 0)]
        channel = data.get('channel', '').strip()
        emoji = data.get('emoji', '👍')
        post_count = safe_int(data.get('post_count'), 10, minimum=1, maximum=MAX_BULK_COUNT)

        if not channel:
            return jsonify({'success': False, 'error': 'Channel/group required'}), 400
        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'}), 400

        valid_accounts = []
        for aid in account_ids:
            acc = TelegramAccount.query.get(aid)
            if acc and acc.user_id == current_user.id and acc.session_string and acc.session_string not in ('demo', 'active'):
                valid_accounts.append(acc)

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        task_id = str(uuid.uuid4())
        task_progress[task_id] = {
            'status': 'running',
            'total_reacted': 0,
            'total_failed': 0,
            'completed': 0,
            'total': len(valid_accounts),
            'results': []
        }
        user_id = current_user.id

        def run_react(acc):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)

                async def _react():
                    import random as _random
                    from telethon.tl.functions.messages import SendReactionRequest
                    from telethon.tl.types import ReactionEmoji

                    # Clean emojis — remove variation selectors that Telegram rejects
                    def _clean_emoji(e):
                        return e.replace('\ufe0f','').replace('\ufe0e','').strip()

                    # Only plain emoji (no variation selector) — Telegram requires these exact forms
                    RANDOM_EMOJIS = ['👍','❤','🔥','😂','🎉','😮','👏','🤩','💯','🏆','⚡','💪','🥰','👀','🎊','🌟','💥','🎯','🚀','✨']
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    reacted = 0
                    failed = 0
                    try:
                        ch_name = parse_channel_username(channel)
                        entity = await client.get_entity(ch_name)
                        async for msg in client.iter_messages(entity, limit=post_count):
                            # Skip service/system messages that don't have reactions
                            if not msg or not msg.id or not msg.text and not msg.media:
                                continue
                            try:
                                raw_pick = _random.choice(RANDOM_EMOJIS) if emoji == 'random' else emoji
                                pick     = _clean_emoji(raw_pick)
                                try:
                                    await client(SendReactionRequest(
                                        peer=entity,
                                        msg_id=msg.id,
                                        reaction=[ReactionEmoji(emoticon=pick)],
                                        add_to_recent=True
                                    ))
                                except TypeError:
                                    # Older Telethon: no add_to_recent param
                                    await client(SendReactionRequest(
                                        peer=entity,
                                        msg_id=msg.id,
                                        reaction=[ReactionEmoji(emoticon=pick)]
                                    ))
                                reacted += 1
                                await asyncio.sleep(_random.uniform(1.2, 2.8))
                            except FloodWaitError as e:
                                await asyncio.sleep(min(e.seconds, 30))
                            except Exception as re:
                                err_s = str(re)
                                if 'REACTION_INVALID' in err_s or 'only emoji' in err_s.lower():
                                    logger.warning(f"react: emoji '{pick}' rejected — skipping msg {msg.id}")
                                    failed += 1
                                    continue
                                logger.warning(f"react msg {msg.id} via {acc.phone}: {re}")
                                failed += 1
                    except Exception as e:
                        logger.warning(f"react_posts entity/connect error {acc.phone}: {e}")
                    finally:
                        await client.disconnect()
                    return reacted, failed

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    reacted, failed = loop.run_until_complete(_react())
                    task_progress[task_id]['total_reacted'] += reacted
                    task_progress[task_id]['total_failed'] += failed
                    task_progress[task_id]['results'].append({'account': acc.phone, 'reacted': reacted, 'failed': failed})
                    log_activity(user_id, f"Reacted {emoji} to {reacted} posts in {channel} via {acc.phone}", 'success', acc.id)
                except Exception as e:
                    task_progress[task_id]['results'].append({'account': acc.phone, 'reacted': 0, 'error': str(e)})
                finally:
                    loop.close()
                    task_progress[task_id]['completed'] += 1
                    if task_progress[task_id]['completed'] >= len(valid_accounts):
                        task_progress[task_id]['status'] = 'completed'

        for acc in valid_accounts:
            threading.Thread(target=run_react, args=(acc,), daemon=True).start()

        return jsonify({'success': True, 'task_id': task_id,
                        'message': f'Reacting to last {post_count} posts using {len(valid_accounts)} accounts'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/bot/react_task_status/<task_id>')
@login_required
def react_task_status(task_id):
    progress = task_progress.get(task_id)
    if not progress:
        return jsonify({'success': False, 'error': 'Task not found'})
    return jsonify({'success': True, 'progress': progress})


# ─── LEAVE GROUPS ─────────────────────────────────────────────────────────────

@app.route('/bot/leave_groups', methods=['POST'])
@login_required
def leave_groups():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        group_links = [l.strip() for l in data.get('group_links', []) if l.strip()]

        if not group_links:
            return jsonify({'success': False, 'error': 'Enter group links/usernames'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _leave():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            left = 0
            failed = []
            for link in group_links:
                try:
                    username = link.split('/')[-1].lstrip('@')
                    entity = await client.get_entity(username)
                    await client(functions.channels.LeaveChannelRequest(channel=entity))
                    left += 1
                    await asyncio.sleep(1)
                except FloodWaitError as e:
                    await asyncio.sleep(min(e.seconds, 30))
                except Exception as e:
                    failed.append({'link': link, 'error': str(e)[:80]})
            await client.disconnect()
            return left, failed

        left, failed = run_async(_leave())
        account.last_used = datetime.utcnow()
        db.session.commit()

        log_activity(current_user.id, f"Left {left} groups ({len(failed)} failed)", 'success', account_id)
        return jsonify({'success': True, 'left': left, 'failed': failed,
                        'message': f'Left {left} groups successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── MESSAGE FORWARDER ────────────────────────────────────────────────────────

@app.route('/bot/forward_messages', methods=['POST'])
@login_required
def forward_messages():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        source_chat = clean_str(data.get('source_chat'))
        target_chats_raw = data.get('target_chats', '')
        if isinstance(target_chats_raw, list):
            target_chats = clean_str_list(target_chats_raw)
        else:
            target_chats = clean_str_list(target_chats_raw, split_lines_only=True)
        target_chats = dedupe_preserve(target_chats)
        msg_count = safe_int(data.get('msg_count'), 5, minimum=0, maximum=MAX_FORWARD_MESSAGES)
        delay = safe_int(data.get('delay'), 2, minimum=1, maximum=MAX_DELAY_SECONDS)

        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not source_chat:
            return jsonify({'success': False, 'error': 'Source chat required'}), 400
        if not target_chats:
            return jsonify({'success': False, 'error': 'At least one target chat required'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'}), 400
        if account_busy(account_id):
            return jsonify({'success': False, 'error': 'Another campaign is already using this account.'}), 409

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        user_id = current_user.id
        sess = account.session_string
        task_key = task_run_start(user_id, 'forwarder', len(target_chats) * msg_count)
        lock = get_account_lock(account_id)

        def run_forward():
            with lock:
                with app.app_context():
                    proxy = get_proxy_for_account(user_id, account_id)
                    result = {'forwarded': 0, 'failed': 0, 'skipped': 0, 'errors': []}

                    async def _forward():
                        client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                        await client.connect()
                        try:
                            source_target = parse_channel_target(source_chat)
                            source_entity = await client.get_entity(
                                source_target['value'] if source_target else source_chat
                            )
                            messages = []
                            async for msg in client.iter_messages(source_entity, limit=msg_count or None):
                                if msg.text or msg.media:
                                    messages.append(msg.id)
                            if not messages:
                                result['errors'].append('no messages found in the source chat')
                                return result
                            for target in target_chats:
                                parsed = parse_channel_target(target)
                                if not parsed:
                                    result['failed'] += 1
                                    result['errors'].append(f'{target}: invalid destination')
                                    continue
                                try:
                                    target_entity = await client.get_entity(parsed['value'])
                                except Exception as e:
                                    result['failed'] += 1
                                    result['errors'].append(friendly_telegram_error(e, target))
                                    continue
                                for message_id in messages:
                                    attempt = 0
                                    while True:
                                        try:
                                            await client.forward_messages(target_entity, message_id, source_entity)
                                            result['forwarded'] += 1
                                            task_run_update(task_key, sent=result['forwarded'])
                                            await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                                            break
                                        except FloodWaitError as e:
                                            attempt += 1
                                            # Wait the full required time (never truncate it), then retry once.
                                            wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                                            await asyncio.sleep(wait)
                                            if attempt >= 2:
                                                result['skipped'] += 1
                                                result['errors'].append(f'{target}: rate limit — skipped after waiting {wait}s')
                                                task_run_update(task_key, skipped=result['skipped'])
                                                break
                                        except Exception as e:
                                            result['failed'] += 1
                                            note = friendly_telegram_error(e, target)
                                            result['errors'].append(note)
                                            task_run_update(task_key, failed=result['failed'], errors=[note])
                                            break
                        finally:
                            await client.disconnect()
                        return result

                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(_forward())
                        final_status = 'completed' if result['forwarded'] and not result['failed'] else (
                            'failed' if not result['forwarded'] else 'partial'
                        )
                        acc = TelegramAccount.query.get(account_id)
                        if acc:
                            acc.messages_sent_today += result['forwarded']
                            acc.total_messages += result['forwarded']
                            acc.last_used = datetime.utcnow()
                            db.session.commit()
                        task_run_finish(
                            task_key, final_status, sent=result['forwarded'],
                            failed=result['failed'], skipped=result['skipped'],
                        )
                        log_activity(
                            user_id,
                            f"Forwarder finished: {result['forwarded']} forwarded, {result['failed']} failed, "
                            f"{result['skipped']} skipped from {source_chat}",
                            'success' if result['forwarded'] else 'failed', account_id,
                        )
                    except Exception as e:
                        task_run_finish(task_key, 'failed', errors=[friendly_telegram_error(e)])
                        logger.error('forward_messages error: %s', e)
                    finally:
                        loop.close()

        threading.Thread(target=run_forward, daemon=True).start()
        return jsonify({'success': True, 'task_key': task_key,
                        'message': f'Forwarding {msg_count} msgs from {source_chat} to {len(target_chats)} chats. Running in background.'})
    except Exception as e:
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


# ─── STORY VIEWER ─────────────────────────────────────────────────────────────

@app.route('/bot/view_stories', methods=['POST'])
@login_required
def view_stories():
    try:
        data = request.get_json()
        account_ids = [safe_int(x, 0) for x in (data.get('account_ids') or []) if safe_int(x, 0)]
        peer = data.get('peer', '').strip().lstrip('@')

        if not peer:
            return jsonify({'success': False, 'error': 'Username/channel required'}), 400
        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'}), 400

        valid_accounts = []
        for aid in account_ids:
            acc = TelegramAccount.query.get(aid)
            if acc and acc.user_id == current_user.id and acc.session_string and acc.session_string not in ('demo', 'active'):
                valid_accounts.append(acc)

        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No properly connected accounts'}), 400

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        user_id = current_user.id
        total_viewed = [0]

        def run_view(acc):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)

                async def _view():
                    from telethon.tl.functions.stories import GetPeerStoriesRequest, ReadStoriesRequest
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    viewed = 0
                    try:
                        peer_entity = await client.get_entity(peer)
                        stories_result = await client(GetPeerStoriesRequest(peer=peer_entity))
                        story_ids = [s.id for s in getattr(stories_result.stories, 'stories', [])]
                        if story_ids:
                            await client(ReadStoriesRequest(peer=peer_entity, max_id=max(story_ids)))
                            viewed = len(story_ids)
                    except Exception as e:
                        logger.warning(f"view_stories {acc.phone}: {e}")
                    finally:
                        await client.disconnect()
                    return viewed

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    viewed = loop.run_until_complete(_view())
                    total_viewed[0] += viewed
                    log_activity(user_id, f"Viewed {viewed} stories of @{peer} via {acc.phone}", 'success', acc.id)
                except Exception as e:
                    logger.error(f"view_stories error {acc.phone}: {e}")
                finally:
                    loop.close()

        threads = [threading.Thread(target=run_view, args=(acc,), daemon=True) for acc in valid_accounts]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        return jsonify({'success': True,
                        'message': f'Viewed stories of @{peer} using {len(valid_accounts)} accounts. Total: {total_viewed[0]} stories viewed.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─── AUTO JOIN WITH CONCURRENT SPEED ─────────────────────────────────────────

@app.route('/bot/auto_join_fast', methods=['POST'])
@login_required
def auto_join_fast():
    """Join groups concurrently (all links at the same time) for speed."""
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        invite_links = [l.strip() for l in data.get('invite_links', []) if l.strip()]

        if not invite_links:
            return jsonify({'success': False, 'error': 'Enter invite links'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'})

        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'})

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def _join_fast():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            joined = 0
            failed = []

            async def join_one(link):
                nonlocal joined
                try:
                    if 't.me/joinchat/' in link or 't.me/+' in link:
                        hash_part = link.split('/')[-1].lstrip('+')
                        await client(functions.messages.ImportChatInviteRequest(hash=hash_part))
                    else:
                        username = link.split('/')[-1].lstrip('@')
                        entity = await client.get_entity(username)
                        await client(functions.channels.JoinChannelRequest(channel=entity))
                    joined += 1
                except UserAlreadyParticipantError:
                    joined += 1
                except FloodWaitError as e:
                    await asyncio.sleep(min(e.seconds, 15))
                except Exception as e:
                    failed.append({'link': link, 'error': str(e)[:80]})

            # Join all links concurrently — no artificial delay
            await asyncio.gather(*[join_one(link) for link in invite_links])

            await client.disconnect()
            return joined, failed

        joined, failed = run_async(_join_fast())
        account.last_used = datetime.utcnow()
        db.session.commit()
        log_activity(current_user.id, f"Fast-joined {joined} groups ({len(failed)} failed)", 'success', account_id)
        return jsonify({'success': True, 'joined': joined, 'failed': failed,
                        'message': f'Fast joined {joined}/{len(invite_links)} groups!'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ─── AI AUTO-REPLY BOT ────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _web_search(query, max_results=3):
    """Search the web using DuckDuckGo and return a short context string."""
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        if not results:
            return ''
        snippets = []
        for r in results[:max_results]:
            title = r.get('title', '')
            body  = r.get('body', '')
            if body:
                snippets.append(f"• {title}: {body[:250]}")
        return "\n".join(snippets)
    except Exception as e:
        logger.warning(f"_web_search error: {e}")
        return ''


def _call_ai_single(provider, api_key, system_prompt, user_message, model=None):
    """Call AI with a SINGLE key. Returns (text_or_None, is_rate_limited)."""
    try:
        if provider == 'gemini':
            full_prompt = f"{system_prompt}\n\nUser: {user_message}\nAssistant:"
            payload = {'contents': [{'parts': [{'text': full_prompt}]}],
                       'generationConfig': {'maxOutputTokens': 500, 'temperature': 0.75}}
            requested = model or 'gemini-2.0-flash'
            FALLBACK_MODELS = [
                requested, 'gemini-2.0-flash', 'gemini-2.0-flash-lite',
                'gemini-2.5-flash', 'gemini-1.5-flash',
            ]
            seen = set()
            all_429 = True
            for mdl in FALLBACK_MODELS:
                if mdl in seen:
                    continue
                seen.add(mdl)
                for api_ver in ('v1beta', 'v1'):
                    url = (f'https://generativelanguage.googleapis.com/{api_ver}'
                           f'/models/{mdl}:generateContent?key={api_key}')
                    try:
                        resp = http_requests.post(url,
                            headers={'Content-Type': 'application/json'},
                            json=payload, timeout=20)
                        if resp.status_code == 200:
                            text = resp.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                            return text, False
                        if resp.status_code == 429:
                            logger.warning(f"Gemini {api_ver}/{mdl} → 429 (quota)")
                            continue
                        if resp.status_code in (404, 500, 503):
                            all_429 = False
                            continue
                        if resp.status_code in (401, 403):
                            logger.warning(f"Gemini bad key {resp.status_code}")
                            return None, False
                        continue
                    except Exception as req_e:
                        logger.warning(f"Gemini request error {mdl}: {req_e}")
                        continue
            return None, all_429   # all_429=True → caller should try next key

        elif provider in ('openai', 'deepseek'):
            base_url = 'https://api.deepseek.com/v1' if provider == 'deepseek' else 'https://api.openai.com/v1'
            default_model = 'deepseek-chat' if provider == 'deepseek' else 'gpt-4o-mini'
            mdl = model or default_model
            resp = http_requests.post(
                f'{base_url}/chat/completions',
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json={'model': mdl,
                      'messages': [{'role': 'system', 'content': system_prompt},
                                   {'role': 'user',   'content': user_message}],
                      'max_tokens': 500},
                timeout=30)
            if resp.status_code == 200:
                return resp.json()['choices'][0]['message']['content'].strip(), False
            if resp.status_code == 429:
                return None, True  # rate limited
            logger.warning(f"{provider} error {resp.status_code}: {resp.text[:300]}")
            return None, False
    except Exception as e:
        logger.error(f"_call_ai_single ({provider}): {e}")
    return None, False


def _call_ai(provider, api_key_or_list, system_prompt, user_message, model=None, enable_search=True):
    """Unified AI caller with multi-key rotation, DeepSeek support, and web-search fallback.

    api_key_or_list: single key string OR list/newline-separated multi-key string.
    enable_search: if True, bot will search the web when it says [SEARCH: ...].
    """
    system_prompt = system_prompt or (
        "You are a real person. Reply like a human — casual, short, natural. "
        "Never say you are an AI or bot. Match the language and tone of the person.\n"
        "If you need current information or prices, write [SEARCH: your search query] "
        "on a line by itself and you will receive search results to help answer."
    )

    # Build key list
    if isinstance(api_key_or_list, list):
        keys = [k.strip() for k in api_key_or_list if k.strip()]
    else:
        # Support newline or comma-separated keys
        raw = str(api_key_or_list or '')
        keys = [k.strip() for k in raw.replace(',', '\n').splitlines() if k.strip()]
    if not keys:
        return None

    # Try each key
    now = time.time()
    for ki, key in enumerate(keys):
        # Skip keys whose quota is known-exhausted (cache for 90s)
        _kh = hash(key) & 0xFFFFFFFF
        if _quota_exhausted_until.get(_kh, 0) > now:
            logger.debug(f"AI key #{ki+1} quota-cached, skipping for {int(_quota_exhausted_until[_kh]-now)}s")
            continue
        text, rate_limited = _call_ai_single(provider, key, system_prompt, user_message, model)
        if text is not None:
            # ── Web search fallback ──────────────────────────────
            if enable_search and '[SEARCH:' in text:
                import re as _re
                m = _re.search(r'\[SEARCH:\s*(.+?)\]', text, _re.IGNORECASE)
                if m:
                    query = m.group(1).strip()
                    logger.info(f"AI triggered web search: {query}")
                    snippets = _web_search(query, max_results=4)
                    if snippets:
                        enriched_msg = (
                            f"{user_message}\n\n"
                            f"[Web search results for '{query}']\n{snippets}"
                        )
                        text2, _ = _call_ai_single(provider, key, system_prompt, enriched_msg, model)
                        if text2:
                            text = text2
                # Remove any remaining [SEARCH:...] tags from final reply
                text = _re.sub(r'\[SEARCH:[^\]]*\]', '', text).strip()
            return text
        if not rate_limited:
            # Non-rate-limit failure (bad key, model error) — try next key anyway
            logger.warning(f"AI key #{ki+1} failed (non-quota), trying next key if available")
        else:
            # Mark this key as quota-exhausted for 90 seconds
            _quota_exhausted_until[_kh] = now + 90
            logger.info(f"AI key #{ki+1} quota exhausted (429), cached for 90s, rotating to next key")

    logger.warning(f"_call_ai: all {len(keys)} key(s) exhausted for provider={provider}")
    return None


def _call_openai(api_key, system_prompt, user_message, model='gpt-4o-mini'):
    """Backward-compat wrapper."""
    return _call_ai('openai', api_key, system_prompt, user_message, model)


@app.route('/bot/auto_reply/global_config', methods=['GET', 'POST'])
@login_required
def auto_reply_global_config():
    """Admin-only: get/set global AI provider, key, model."""
    if not is_admin_user():
        return jsonify({'success': False, 'error': 'Admin only'})
    if request.method == 'POST':
        data = request.get_json()
        all_keys = ('ar_provider', 'ar_api_keys', 'ar_model', 'ar_offline_mins',
                    'ar_enable_search', 'ar_voice_recv', 'ar_voice_reply', 'ar_tts_lang')
        for k in all_keys:
            if k in data and data[k] is not None:
                GlobalSetting.set_val(k, str(data[k]).strip())
        db.session.commit()
        return jsonify({'success': True, 'message': 'Global AI config saved ✅'})
    raw_keys  = GlobalSetting.get('ar_api_keys', '') or ''
    key_list  = [k.strip() for k in raw_keys.replace(',', '\n').splitlines() if k.strip()]
    key_count = len(key_list)
    key_hint  = f'****{key_list[0][-4:]}' if key_list else ''
    return jsonify({
        'success':        True,
        'provider':       GlobalSetting.get('ar_provider', 'gemini'),
        'api_keys':       raw_keys,
        'api_key_hint':   key_hint,
        'api_key_set':    key_count > 0,
        'key_count':      key_count,
        'model':          GlobalSetting.get('ar_model', 'gemini-2.5-flash'),
        'offline_mins':   GlobalSetting.get('ar_offline_mins', '10'),
        'enable_search':  GlobalSetting.get('ar_enable_search', '1') == '1',
        'voice_recv':     GlobalSetting.get('ar_voice_recv', '1') == '1',
        'voice_reply':    GlobalSetting.get('ar_voice_reply', '0') == '1',
        'tts_lang':       GlobalSetting.get('ar_tts_lang', 'auto') or 'auto',
    })


@app.route('/bot/auto_reply/save', methods=['POST'])
@login_required
def auto_reply_save():
    data             = request.get_json()
    account_id       = safe_int(data.get('account_id'), 0)
    system_prompt    = data.get('system_prompt', '').strip()
    enabled          = bool(data.get('enabled', False))
    service_keywords = data.get('service_keywords', '').strip()
    acc = get_account_for_current_user(account_id)
    if not acc:
        return jsonify({'success': False, 'error': 'Account not found'})
    bot = AutoReplyBot.query.filter_by(account_id=account_id).first()
    if not bot:
        bot = AutoReplyBot(user_id=current_user.id, account_id=account_id)
        db.session.add(bot)
    bot.system_prompt    = system_prompt
    bot.service_keywords = service_keywords
    bot.enabled          = enabled
    db.session.commit()
    status = '🟢 Active' if enabled else '⚫ Disabled'
    return jsonify({'success': True, 'message': f'Auto-reply bot saved — {status}'})


@app.route('/bot/auto_reply/analyze_account', methods=['POST'])
@login_required
def auto_reply_analyze_account():
    """Scan account's own channels/groups, build AI context profile, save to AutoReplyBot."""
    data       = request.get_json()
    account_id = safe_int(data.get('account_id'), 0)
    acc        = get_account_for_current_user(account_id)
    if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
        return jsonify({'success': False, 'error': 'Account not connected'})
    api_id, api_hash = get_api_credentials(current_user.id)
    if not api_id:
        return jsonify({'success': False, 'error': 'API credentials not configured'})

    g_provider = GlobalSetting.get('ar_provider', 'gemini')
    raw_keys   = GlobalSetting.get('ar_api_keys', '') or ''
    g_keys     = [k.strip() for k in raw_keys.replace(',', '\n').splitlines() if k.strip()]
    g_model    = GlobalSetting.get('ar_model', 'gemini-2.5-flash')
    if not g_keys:
        return jsonify({'success': False, 'error': 'No AI API key configured in Global AI Config'})

    proxy = get_proxy_for_account(current_user.id, account_id)

    async def _do_analyze():
        from telethon import TelegramClient
        from telethon.tl.types import Channel, Chat
        client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
        await client.connect()
        collected_posts = []
        channel_names   = []
        try:
            dialogs = await client.get_dialogs(limit=100)
            me = await client.get_me()
            for dlg in dialogs:
                ent = dlg.entity
                # Only admin-owned channels or groups
                is_channel = isinstance(ent, Channel) and getattr(ent, 'broadcast', False)
                is_group   = isinstance(ent, (Channel, Chat))
                if not (is_channel or is_group):
                    continue
                # Check admin rights
                try:
                    perms = await client.get_permissions(ent, me)
                    if not (getattr(perms, 'is_admin', False) or
                            getattr(perms, 'is_creator', False)):
                        continue
                except Exception:
                    continue
                channel_names.append(dlg.name)
                msgs = await client.get_messages(ent, limit=25)
                for m in msgs:
                    if m.text and len(m.text) > 20:
                        collected_posts.append(f"[{dlg.name}] {m.text[:400]}")
                if len(collected_posts) >= 150:
                    break
        finally:
            await client.disconnect()
        return collected_posts, channel_names

    import asyncio as _aio
    loop = _aio.new_event_loop()
    _aio.set_event_loop(loop)
    try:
        posts, ch_names = loop.run_until_complete(_do_analyze())
    except Exception as e:
        return jsonify({'success': False, 'error': f'Telegram error: {str(e)[:200]}'})
    finally:
        loop.close()

    if not posts:
        return jsonify({'success': False,
                        'error': 'No posts found. Make sure this account is admin in at least one channel/group.'})

    # Build analysis prompt
    sample = "\n".join(posts[:80])
    analysis_prompt = (
        "You are an expert analyst. Based on the following Telegram posts from a person's own channels/groups, "
        "write a concise profile (3-5 sentences) that describes:\n"
        "1. What this person does (business, services, topics)\n"
        "2. Their communication style and language(s) used\n"
        "3. Their target audience\n"
        "4. Any key products, services, or topics they focus on\n\n"
        "Use this as CONTEXT for an AI that will reply on their behalf. Be specific and factual.\n\n"
        "POSTS:\n" + sample
    )

    context = _call_ai(g_provider, g_keys, "You are a profile analyst.", analysis_prompt, g_model, enable_search=False)
    if not context:
        return jsonify({'success': False, 'error': 'AI analysis failed. Check your API key.'})

    bot = AutoReplyBot.query.filter_by(account_id=account_id).first()
    if not bot:
        bot = AutoReplyBot(user_id=current_user.id, account_id=account_id)
        db.session.add(bot)
    bot.account_context = context
    db.session.commit()

    return jsonify({
        'success':       True,
        'context':       context,
        'channels_found': ch_names[:10],
        'posts_analyzed': len(posts),
        'message':       f'✅ Analyzed {len(posts)} posts from {len(ch_names)} channels/groups'
    })


@app.route('/bot/auto_reply/get')
@login_required
def auto_reply_get():
    bots = AutoReplyBot.query.filter_by(user_id=current_user.id).all()
    result = []
    for b in bots:
        acc = TelegramAccount.query.get(b.account_id)
        provider = getattr(b, 'provider', 'openai') or 'openai'
        has_key  = bool(b.gemini_key) if provider == 'gemini' else bool(b.openai_key)
        result.append({'id': b.id, 'account_id': b.account_id,
                       'phone': acc.phone if acc else '?',
                       'enabled': b.enabled, 'model': b.model,
                       'provider': provider,
                       'system_prompt': b.system_prompt or '',
                       'service_keywords': getattr(b, 'service_keywords', '') or '',
                       'account_context': getattr(b, 'account_context', '') or '',
                       'has_key': has_key})
    return jsonify({'success': True, 'bots': result})


@app.route('/bot/auto_reply/test_key', methods=['POST'])
@login_required
def auto_reply_test_key():
    """Test Gemini or OpenAI API key and find working model."""
    data     = request.get_json()
    provider = data.get('provider', 'gemini')
    api_key  = data.get('api_key', '').strip()
    if not api_key:
        return jsonify({'success': False, 'error': 'API key required'})

    if provider == 'gemini':
        payload = {'contents': [{'parts': [{'text': 'Say WORKING'}]}]}
        headers = {'Content-Type': 'application/json'}

        # Step 1: fetch available models from the API itself
        live_models = []
        for api_ver in ('v1beta', 'v1'):
            try:
                lr = http_requests.get(
                    f'https://generativelanguage.googleapis.com/{api_ver}/models?key={api_key}',
                    timeout=10)
                if lr.status_code in (401, 403):
                    return jsonify({'success': False, 'error': '❌ Invalid API key — please check and try again'})
                if lr.status_code == 200:
                    for m in lr.json().get('models', []):
                        if 'generateContent' in m.get('supportedGenerationMethods', []):
                            mdl_name = m['name'].split('/')[-1]
                            live_models.append((api_ver, mdl_name))
                    if live_models:
                        break
            except Exception:
                continue

        # Step 2: prefer non-TTS, non-vision models for chat
        def model_priority(item):
            _, m = item
            if 'tts' in m: return 99
            if 'vision' in m: return 90
            if '2.5' in m: return 0
            if '2.0' in m: return 1
            if '1.5' in m: return 2
            return 5
        live_models.sort(key=model_priority)

        # Step 3: fallback list in case listing fails
        FALLBACK = [
            ('v1beta', 'gemini-2.5-flash'), ('v1beta', 'gemini-2.5-pro'),
            ('v1beta', 'gemini-2.0-flash'), ('v1beta', 'gemini-2.0-flash-001'),
            ('v1beta', 'gemini-2.0-flash-lite'), ('v1beta', 'gemini-2.0-flash-lite-001'),
            ('v1beta', 'gemini-1.5-flash'), ('v1beta', 'gemini-1.5-flash-latest'),
            ('v1',     'gemini-2.5-flash'), ('v1', 'gemini-2.0-flash'),
        ]
        candidates = live_models if live_models else FALLBACK

        # Step 4: try each candidate
        tried, errors = [], []
        for api_ver, mdl in candidates:
            url = (f'https://generativelanguage.googleapis.com/{api_ver}'
                   f'/models/{mdl}:generateContent?key={api_key}')
            try:
                resp = http_requests.post(url, headers=headers, json=payload, timeout=12)
                tried.append(f'{mdl}({resp.status_code})')
                if resp.status_code == 200:
                    return jsonify({'success': True,
                                    'message': f'✅ Gemini key works! Model: {mdl}',
                                    'model': mdl, 'api_ver': api_ver})
                if resp.status_code in (401, 403):
                    return jsonify({'success': False, 'error': '❌ Invalid API key — please check and try again'})
                if resp.status_code == 400:
                    err = resp.json().get('error', {}).get('message', '')
                    errors.append(f'{mdl}: {err}')
            except Exception as e:
                errors.append(f'{mdl}: {e}')

        avail_names = ', '.join(m for _, m in live_models[:8]) if live_models else 'none listed'
        tried_str   = ', '.join(tried[:8])
        return jsonify({'success': False,
                        'error': (f'❌ All models failed. Available: {avail_names}. '
                                  f'Tried: {tried_str}. '
                                  f'This key may need billing setup or be region-restricted.')})
    elif provider == 'deepseek':
        try:
            resp = http_requests.post(
                'https://api.deepseek.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json={'model': 'deepseek-chat', 'messages': [{'role': 'user', 'content': 'Hi'}], 'max_tokens': 5},
                timeout=20)
            if resp.status_code == 200:
                return jsonify({'success': True, 'message': '✅ DeepSeek key works! deepseek-chat ready.', 'model': 'deepseek-chat'})
            err = resp.json().get('error', {}).get('message', resp.text[:200])
            return jsonify({'success': False, 'error': f'DeepSeek: {err}'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    else:
        # OpenAI test
        try:
            resp = http_requests.post(
                'https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-4o-mini', 'messages': [{'role': 'user', 'content': 'Hi'}], 'max_tokens': 5},
                timeout=15)
            if resp.status_code == 200:
                return jsonify({'success': True, 'message': '✅ OpenAI key works! GPT-4o-mini ready.', 'model': 'gpt-4o-mini'})
            err = resp.json().get('error', {}).get('message', resp.text[:200])
            return jsonify({'success': False, 'error': f'OpenAI: {err}'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})


@app.route('/bot/auto_reply/scan_account', methods=['POST'])
@login_required
def auto_reply_scan_account():
    """Scan a Telegram account to auto-detect services and generate a smart system prompt."""
    data       = request.get_json()
    account_id = safe_int(data.get('account_id'), 0)
    provider   = data.get('provider', 'gemini')
    ai_key     = data.get('ai_key', '').strip()

    # Fall back to global AI config if no key provided in request
    if not ai_key or ai_key == 'GLOBAL':
        provider = GlobalSetting.get('ar_provider', 'gemini')
        raw_keys = GlobalSetting.get('ar_api_keys', '') or ''
        key_list = [k.strip() for k in raw_keys.replace(',', '\n').splitlines() if k.strip()]
        ai_key   = key_list[0] if key_list else ''

    acc = get_account_for_current_user(account_id)
    if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
        return jsonify({'success': False, 'error': 'Account not connected properly'})
    if not ai_key:
        return jsonify({'success': False, 'error': 'No AI key configured — admin must set Global AI Config first'})

    api_id, api_hash = get_api_credentials(current_user.id)
    if not api_id:
        return jsonify({'success': False, 'error': 'API credentials not set'})

    proxy = get_proxy_for_account(current_user.id, account_id)

    async def _scan():
        client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
        await client.connect()
        profile_lines = []
        channels_info = []
        recent_sent   = []
        try:
            me = await client.get_me()
            name = f"{me.first_name or ''} {me.last_name or ''}".strip()
            profile_lines.append(f"Name: {name}")
            if me.username:
                profile_lines.append(f"Username: @{me.username}")
            if hasattr(me, 'about') and me.about:
                profile_lines.append(f"Bio: {me.about}")

            # Scan dialogs: collect admin channels + recent outgoing messages
            dialogs = await client.get_dialogs(limit=60)
            for dlg in dialogs:
                # Admin channels/groups
                if hasattr(dlg.entity, 'megagroup') or hasattr(dlg.entity, 'broadcast'):
                    try:
                        from telethon.tl.types import Channel
                        if isinstance(dlg.entity, Channel):
                            ch_info = f"Channel/Group: {dlg.name}"
                            if dlg.entity.username:
                                ch_info += f" (@{dlg.entity.username})"
                            full = await client(functions.channels.GetFullChannelRequest(dlg.entity))
                            if full.full_chat.about:
                                ch_info += f"\n  Description: {full.full_chat.about[:300]}"
                            channels_info.append(ch_info)
                    except Exception:
                        pass

                # Collect recent outgoing messages from user DMs and groups
                if len(recent_sent) < 40:
                    try:
                        msgs = await client.get_messages(dlg.entity, limit=8)
                        for m in msgs:
                            if m.out and m.text and len(m.text) > 10:
                                recent_sent.append(m.text[:200])
                                if len(recent_sent) >= 40:
                                    break
                    except Exception:
                        pass
        finally:
            await client.disconnect()
        return profile_lines, channels_info, recent_sent

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        profile_lines, channels_info, recent_sent = loop.run_until_complete(_scan())
    except Exception as e:
        return jsonify({'success': False, 'error': f'Telegram scan failed: {e}'})
    finally:
        loop.close()

    # Build context for AI analysis
    context_parts = []
    if profile_lines:
        context_parts.append("== PROFILE ==\n" + "\n".join(profile_lines))
    if channels_info:
        context_parts.append("== CHANNELS/GROUPS ==\n" + "\n\n".join(channels_info[:5]))
    if recent_sent:
        context_parts.append("== RECENT MESSAGES SENT ==\n" + "\n---\n".join(recent_sent[:20]))

    if not context_parts:
        return jsonify({'success': False, 'error': 'Could not find enough data on this account to scan'})

    full_context = "\n\n".join(context_parts)

    analysis_prompt = f"""Based on this Telegram account data, extract:
1. What services or products does this person/business offer? (List each as a short phrase, comma-separated)
2. What is their profession or niche?
3. Write a short natural system prompt (2-3 sentences) for an AI to impersonate this person in DMs — human-like, helpful about their services, never admitting to be a bot.

Telegram Account Data:
{full_context[:3000]}

Reply in this exact JSON format (no markdown):
{{"services": "service1, service2, service3", "profession": "their niche", "system_prompt": "You are [name]..."}}"""

    ai_response = _call_ai(provider, ai_key,
                           "You are a business intelligence analyst. Always reply with valid JSON only.",
                           analysis_prompt, None)

    if not ai_response:
        return jsonify({'success': False, 'error': 'AI analysis failed — check your API key and try again'})

    try:
        # Extract JSON from response
        import re as _re
        json_match = _re.search(r'\{.*\}', ai_response, _re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
        else:
            result = json.loads(ai_response)
        return jsonify({
            'success':         True,
            'services':        result.get('services', ''),
            'profession':      result.get('profession', ''),
            'system_prompt':   result.get('system_prompt', ''),
            'scanned_items': {
                'profile': len(profile_lines) > 0,
                'channels': len(channels_info),
                'messages': len(recent_sent)
            }
        })
    except Exception as parse_err:
        # AI returned text but not clean JSON — still try to extract services
        return jsonify({'success': True, 'services': '', 'profession': '',
                        'system_prompt': ai_response[:500],
                        'scanned_items': {'profile': True, 'channels': len(channels_info), 'messages': len(recent_sent)}})


@app.route('/bot/auto_reply/clear_context', methods=['POST'])
@login_required
def auto_reply_clear_context():
    """Clear the AI-generated account context profile for an account."""
    data       = request.get_json()
    account_id = safe_int(data.get('account_id'), 0)
    bot = AutoReplyBot.query.filter_by(account_id=account_id, user_id=current_user.id).first()
    if bot:
        bot.account_context = None
        db.session.commit()
    return jsonify({'success': True})


@app.route('/bot/auto_reply/delete/<int:bot_id>', methods=['DELETE'])
@login_required
def auto_reply_delete(bot_id):
    bot = AutoReplyBot.query.filter_by(id=bot_id, user_id=current_user.id).first()
    if not bot:
        return jsonify({'success': False, 'error': 'Not found'})
    db.session.delete(bot)
    db.session.commit()
    return jsonify({'success': True})


# ── Auto-reply: track bot-sent msg IDs to exclude from "user online" detection ─
_ar_bot_sent    = {}   # {account_id: set(sent_msg_ids)}
_ar_fail_count  = {}   # {(account_id, msg_id): fail_count}  — skip after 2 quota failures
_ar_voice_cache = {}   # {(account_id, msg_id): transcribed_text}
_quota_exhausted_until = {}  # {key_hash: expire_timestamp} — skip quota-dead keys


# ─────────────────────────────────────────────────────────────────────────────
# VOICE HELPERS (speech-to-text & text-to-speech)
# ─────────────────────────────────────────────────────────────────────────────

def _detect_lang(text):
    """Guess language from Unicode characters. Returns 'bn' or 'en'."""
    bn_chars = sum(1 for c in text if '\u0980' <= c <= '\u09FF')
    return 'bn' if bn_chars > len(text) * 0.15 else 'en'


def _transcribe_voice_bytes(audio_bytes, provider='gemini', api_key='', hint_lang=None):
    """Convert voice bytes (OGG/MP3/etc.) to text. Returns text or None."""
    try:
        import io
        from pydub import AudioSegment
        import speech_recognition as sr

        # Convert any format → WAV (speech_recognition needs WAV)
        audio = AudioSegment.from_file(io.BytesIO(audio_bytes))
        wav_io = io.BytesIO()
        audio.export(wav_io, format='wav')
        wav_io.seek(0)

        # Try OpenAI Whisper API first (most accurate)
        if provider == 'openai' and api_key:
            try:
                files = {'file': ('audio.wav', wav_io, 'audio/wav')}
                resp  = http_requests.post(
                    'https://api.openai.com/v1/audio/transcriptions',
                    headers={'Authorization': f'Bearer {api_key}'},
                    data={'model': 'whisper-1'},
                    files=files, timeout=30)
                if resp.status_code == 200:
                    return resp.json().get('text', '').strip()
                wav_io.seek(0)
            except Exception as e:
                logger.warning(f"Whisper API error: {e}")
                wav_io.seek(0)

        # Fallback: Google free Speech Recognition (no key needed)
        recognizer = sr.Recognizer()
        try:
            with sr.AudioFile(wav_io) as src:
                audio_data = recognizer.record(src)
        except Exception:
            return None

        # Try user's hinted language first, then common ones
        langs = []
        if hint_lang:
            langs.append(hint_lang)
        langs += ['bn-BD', 'en-US', 'bn-IN']
        seen = set()
        for lang in langs:
            if lang in seen:
                continue
            seen.add(lang)
            try:
                text = recognizer.recognize_google(audio_data, language=lang)
                if text:
                    return text.strip()
            except Exception:
                continue
        return None
    except Exception as e:
        logger.warning(f"_transcribe_voice_bytes error: {e}")
        return None


def _text_to_voice_ogg(text, lang=None):
    """Convert text to an OGG OPUS voice file. Returns bytes or None."""
    try:
        import io
        from gtts import gTTS
        from pydub import AudioSegment

        if not lang or lang == 'auto':
            lang = _detect_lang(text)

        tts    = gTTS(text=text, lang=lang, slow=False)
        mp3_io = io.BytesIO()
        tts.write_to_fp(mp3_io)
        mp3_io.seek(0)

        audio  = AudioSegment.from_file(mp3_io, format='mp3')
        ogg_io = io.BytesIO()
        audio.export(ogg_io, format='ogg', codec='libopus',
                     parameters=['-vbr', 'on'])
        return ogg_io.getvalue()
    except Exception as e:
        logger.warning(f"_text_to_voice_ogg error: {e}")
        return None


def run_auto_reply_bot():
    """Background thread: poll for new DMs and reply with AI."""

    def worker():
        while True:
            time.sleep(8)   # check every 8 seconds for faster response
            try:
                with app.app_context():
                    # ── Load global AI config (admin-controlled) ───────────────
                    g_provider    = GlobalSetting.get('ar_provider', 'gemini')
                    raw_keys      = GlobalSetting.get('ar_api_keys', '') or ''
                    g_keys        = [k.strip() for k in raw_keys.replace(',', '\n').splitlines() if k.strip()]
                    g_model       = GlobalSetting.get('ar_model', 'gemini-2.5-flash')
                    offline_mins  = int(GlobalSetting.get('ar_offline_mins', '10') or 10)
                    en_search     = GlobalSetting.get('ar_enable_search', '1') == '1'
                    en_voice_recv = GlobalSetting.get('ar_voice_recv', '1') == '1'
                    en_voice_reply= GlobalSetting.get('ar_voice_reply', '0') == '1'
                    tts_lang      = GlobalSetting.get('ar_tts_lang', 'auto') or 'auto'

                    if not g_keys:
                        logger.debug("Auto-reply: no global AI key configured, skipping")
                        continue

                    bots = AutoReplyBot.query.filter_by(enabled=True).all()
                    for bot in bots:
                        acc = TelegramAccount.query.get(bot.account_id)
                        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
                            continue
                        api_id, api_hash = get_api_credentials(bot.user_id)
                        if not api_id:
                            continue

                        account_id   = bot.account_id
                        proxy        = get_proxy_for_account(bot.user_id, account_id)
                        bot_id       = bot.id
                        base_prompt     = bot.system_prompt or "You are a real person. Reply like a human — casual, short, natural. Never say you are an AI or bot. Match the language and tone of the person you are chatting with."
                        svc_kw          = getattr(bot, 'service_keywords', '') or ''
                        account_context = getattr(bot, 'account_context', '') or ''
                        system_prompt   = base_prompt
                        if svc_kw:
                            system_prompt += (
                                f"\n\nBackground: You personally offer these services — {svc_kw}. "
                                f"IMPORTANT: Only bring up or discuss your services when the person ASKS about them or shows clear interest. "
                                f"For greetings (Hi, Hello, Hey, etc.) just reply warmly and naturally — do NOT mention services unprompted. "
                                f"When services are relevant, answer knowledgeably as if you personally provide them. "
                                f"Vary your wording naturally — never sound repetitive or like a bot."
                            )
                        if account_context:
                            system_prompt += (
                                f"\n\n[ACCOUNT PROFILE — use as context, never directly quote]:\n{account_context[:600]}"
                            )

                        last_msg_id  = bot.last_msg_id or 0
                        sess         = acc.session_string
                        bot_sent     = _ar_bot_sent.get(account_id, set())

                        async def _check(sess=sess, api_id=api_id, api_hash=api_hash, proxy=proxy,
                                         last_msg_id=last_msg_id, system_prompt=system_prompt,
                                         account_id=account_id, bot_sent=bot_sent,
                                         g_provider=g_provider, g_keys=g_keys,
                                         g_model=g_model, offline_mins=offline_mins,
                                         en_search=en_search,
                                         en_voice_recv=en_voice_recv,
                                         en_voice_reply=en_voice_reply,
                                         tts_lang=tts_lang):
                            import random as _rnd
                            import io as _io
                            from telethon.tl.types import DocumentAttributeAudio
                            client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                            await client.connect()
                            replied_to = []
                            new_max_id = last_msg_id
                            try:
                                dialogs = await client.get_dialogs(limit=50)

                                # ── STEP 1: Offline detection ──────────────────────────
                                now_utc        = datetime.utcnow()
                                user_is_online = False
                                for chk_dlg in dialogs[:20]:
                                    try:
                                        chk_msgs = await client.get_messages(chk_dlg.entity, limit=5)
                                        for cm in chk_msgs:
                                            if not cm.out or cm.id in bot_sent:
                                                continue
                                            if cm.date:
                                                age = (now_utc - cm.date.replace(tzinfo=None)).total_seconds()
                                                if age < offline_mins * 60:
                                                    user_is_online = True
                                                    break
                                    except Exception:
                                        pass
                                    if user_is_online:
                                        break

                                if user_is_online:
                                    logger.info(f"Auto-reply [{account_id}]: user ONLINE — silent")
                                    return new_max_id, replied_to

                                # ── STEP 2: Find new incoming DMs and reply ────────────
                                for dlg in dialogs:
                                    if not dlg.is_user:
                                        continue
                                    dlg_latest_id = dlg.message.id if dlg.message else 0
                                    if dlg_latest_id <= last_msg_id:
                                        continue

                                    # Fetch conversation history (last 8 messages)
                                    msgs = await client.get_messages(dlg.entity, limit=8)

                                    # Find the newest unprocessed incoming message
                                    target_msg   = None
                                    is_voice_msg = False
                                    for msg in msgs:
                                        if msg.id <= last_msg_id:
                                            break
                                        if msg.out:
                                            continue
                                        # Check if it's a voice note
                                        if msg.media and hasattr(msg.media, 'document'):
                                            for attr in getattr(msg.media.document, 'attributes', []):
                                                if isinstance(attr, DocumentAttributeAudio) and getattr(attr, 'voice', False):
                                                    if en_voice_recv:
                                                        target_msg   = msg
                                                        is_voice_msg = True
                                                    break
                                            if target_msg:
                                                break
                                        # Regular text message
                                        if msg.text:
                                            target_msg = msg
                                            break

                                    if not target_msg:
                                        continue

                                    # ── Transcribe voice if needed (with cache) ───────
                                    user_text = None
                                    if is_voice_msg:
                                        _vc_key = (account_id, target_msg.id)
                                        if _vc_key in _ar_voice_cache:
                                            user_text = _ar_voice_cache[_vc_key]
                                            logger.info(f"Auto-reply [{account_id}]: using cached transcription for {target_msg.id}")
                                        else:
                                            logger.info(f"Auto-reply [{account_id}]: voice msg {target_msg.id} from {dlg.name} — transcribing")
                                            loop_here = asyncio.get_event_loop()
                                            voice_bytes = await client.download_media(target_msg, bytes)
                                            first_key   = g_keys[0] if g_keys else ''
                                            user_text   = await loop_here.run_in_executor(
                                                None,
                                                lambda vb=voice_bytes, pk=first_key: _transcribe_voice_bytes(vb, g_provider, pk)
                                            )
                                            if not user_text:
                                                logger.warning(f"Voice transcription failed for {target_msg.id}, skipping")
                                                new_max_id = max(new_max_id, target_msg.id)
                                                continue
                                            _ar_voice_cache[_vc_key] = user_text
                                            logger.info(f"Voice transcribed: '{user_text[:80]}'")
                                    else:
                                        user_text = target_msg.text

                                    # ── Build conversation context ────────────────────
                                    history_lines = []
                                    for hm in reversed(msgs[:6]):
                                        if not hm.text:
                                            continue
                                        role = "Me" if hm.out else "Them"
                                        history_lines.append(f"{role}: {hm.text[:300]}")
                                    conv_context = "\n".join(history_lines) if len(history_lines) > 1 else user_text

                                    if is_voice_msg:
                                        conv_context = f"[Voice message]: {user_text}"

                                    logger.info(f"Auto-reply [{account_id}]: processing msg {target_msg.id} from {dlg.name}")

                                    # ── AI call ──────────────────────────────────────
                                    ai_reply = _call_ai(g_provider, g_keys, system_prompt, conv_context, g_model, enable_search=en_search)
                                    if not ai_reply:
                                        _fk = (account_id, target_msg.id)
                                        _ar_fail_count[_fk] = _ar_fail_count.get(_fk, 0) + 1
                                        if _ar_fail_count[_fk] >= 2:
                                            # All keys exhausted twice — skip this message to avoid infinite loop
                                            logger.warning(f"AI quota exhausted for msg {target_msg.id} — skipping (add more API keys)")
                                            _ar_fail_count.pop(_fk, None)
                                            _ar_voice_cache.pop((account_id, target_msg.id), None)
                                            new_max_id = max(new_max_id, target_msg.id)
                                        else:
                                            logger.warning(f"AI reply failed for msg {target_msg.id}, will retry next cycle")
                                        break

                                    # ── Safety: random delay ─────────────────────────
                                    await asyncio.sleep(_rnd.uniform(2, 8))

                                    # ── Send reply (voice or text) ────────────────────
                                    sent = None
                                    if is_voice_msg and en_voice_reply:
                                        # Reply with voice note
                                        loop_here  = asyncio.get_event_loop()
                                        reply_lang = tts_lang if tts_lang != 'auto' else None
                                        ogg_bytes  = await loop_here.run_in_executor(
                                            None,
                                            lambda rt=ai_reply, rl=reply_lang: _text_to_voice_ogg(rt, rl)
                                        )
                                        if ogg_bytes:
                                            try:
                                                async with client.action(dlg.entity, 'record-audio'):
                                                    await asyncio.sleep(_rnd.uniform(1, 3))
                                                # IMPORTANT: BytesIO must have .name so Telegram
                                                # treats it as OGG audio, not an unnamed document
                                                _vbuf = _io.BytesIO(ogg_bytes)
                                                _vbuf.name = 'voice.ogg'
                                                sent = await client.send_file(
                                                    dlg.entity,
                                                    _vbuf,
                                                    voice_note=True)
                                                logger.info(f"Auto-reply [{account_id}]: sent VOICE reply to {dlg.name}")
                                            except Exception as ve:
                                                logger.warning(f"Voice send failed: {ve}, falling back to text")
                                        if not sent:
                                            # Fallback: send as text if voice failed
                                            type_secs = min(len(ai_reply) * 0.045 + _rnd.uniform(1, 3), 8)
                                            try:
                                                async with client.action(dlg.entity, 'typing'):
                                                    await asyncio.sleep(type_secs)
                                            except Exception:
                                                await asyncio.sleep(type_secs)
                                            sent = await client.send_message(dlg.entity, ai_reply)
                                    else:
                                        # Normal text reply
                                        type_secs = min(len(ai_reply) * 0.045 + _rnd.uniform(1, 3), 8)
                                        try:
                                            async with client.action(dlg.entity, 'typing'):
                                                await asyncio.sleep(type_secs)
                                        except Exception:
                                            await asyncio.sleep(type_secs)
                                        sent = await client.send_message(dlg.entity, ai_reply)

                                    if sent:
                                        replied_to.append(target_msg.id)
                                        new_max_id = max(new_max_id, target_msg.id)
                                        if account_id not in _ar_bot_sent:
                                            _ar_bot_sent[account_id] = set()
                                        _ar_bot_sent[account_id].add(sent.id)
                                        # Clean up caches on success
                                        _ar_fail_count.pop((account_id, target_msg.id), None)
                                        _ar_voice_cache.pop((account_id, target_msg.id), None)
                                        logger.info(f"Auto-reply [{account_id}]: replied to {dlg.name} ({'voice' if is_voice_msg and en_voice_reply else 'text'})")
                                    break  # one reply per cycle

                            finally:
                                await client.disconnect()
                            return new_max_id, replied_to

                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            new_max_id, replied = loop.run_until_complete(_check())
                            if new_max_id > last_msg_id or replied:
                                b = AutoReplyBot.query.get(bot_id)
                                if b:
                                    b.last_msg_id = new_max_id
                                    db.session.commit()
                            if replied:
                                log_activity(bot.user_id,
                                             f"AI auto-reply sent to {len(replied)} DM(s)",
                                             'success', bot.account_id)
                        except Exception as e:
                            logger.warning(f"auto_reply_bot {acc.phone}: {e}")
                        finally:
                            loop.close()
            except Exception as e:
                logger.error(f"run_auto_reply_bot outer: {e}")

    t = threading.Thread(target=worker, daemon=True)
    t.start()



# ═══════════════════════════════════════════════════════════════════════════════
# ─── AUTO VIDEO POSTER (ZIP upload OR Telegram Saved Messages → many channels) ─
# ═══════════════════════════════════════════════════════════════════════════════
# Channel ceiling: MAX_AUTO_POST_CHANNELS above is the single source of truth and
# is published to the dashboard through /bot/app_config, so the UI text and the
# backend validation can never disagree.
# Source, queue and counter state live in the database, so a Railway restart (or
# a brand-new service on the same Neon database) resumes instead of losing or
# re-sending videos.

def _resolve_media_root():
    """Pick a writable media root, preferring a persistent Railway volume.

    Railway containers have an ephemeral filesystem, so ZIP videos extracted into
    the app folder are deleted by every redeploy while the campaign row survives in
    PostgreSQL. Setting AUTO_POST_MEDIA_DIR (or attaching a volume mounted at
    /data) makes the uploads durable. Either way, campaign.media_dir keeps working
    because each posted video is also remembered as a Telegram-side copy.
    """
    candidates = []
    configured = (os.environ.get('AUTO_POST_MEDIA_DIR') or '').strip()
    if configured:
        candidates.append(configured)
    candidates.append(os.path.join(os.sep, 'data', 'auto_post_media'))
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'auto_post_media'))
    for candidate in candidates:
        try:
            os.makedirs(candidate, exist_ok=True)
            probe = os.path.join(candidate, '.write_probe')
            with open(probe, 'w', encoding='utf-8') as fh:
                fh.write('ok')
            os.remove(probe)
            return candidate
        except Exception as exc:
            logger.warning('Media root %s is not usable: %s', candidate, exc)
    return candidates[-1]


AUTO_VIDEO_ROOT = _resolve_media_root()
try:
    os.makedirs(AUTO_VIDEO_ROOT, exist_ok=True)
except OSError as exc:                       # pragma: no cover - environment dependent
    logger.warning('Could not create media root %s: %s', AUTO_VIDEO_ROOT, exc)
ALLOWED_VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v'}

# ─── DEVICE UPLOADS FOR THE MESSAGING TOOLS ──────────────────────────────────
# Photos/videos picked straight from the operator's device are stored here and
# the absolute path is handed back to the existing senders (they already accept
# local paths), so no sender had to change. Same candidate order as the
# auto-poster: explicit env dir > /data volume > app directory.
ALLOWED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
ALLOWED_MEDIA_EXTENSIONS = ALLOWED_IMAGE_EXTENSIONS | ALLOWED_VIDEO_EXTENSIONS


def _resolve_messaging_media_root():
    candidates = []
    configured = (os.environ.get('MESSAGING_MEDIA_DIR') or '').strip()
    if configured:
        candidates.append(configured)
    candidates.append(os.path.join(os.sep, 'data', 'messaging_media'))
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'messaging_media'))
    for candidate in candidates:
        try:
            os.makedirs(candidate, exist_ok=True)
            probe = os.path.join(candidate, '.write_probe')
            with open(probe, 'w', encoding='utf-8') as fh:
                fh.write('ok')
            os.remove(probe)
            return candidate
        except Exception:
            continue
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'messaging_media')


MESSAGING_MEDIA_ROOT = _resolve_messaging_media_root()

# campaign_id -> Thread for every round this process is currently running.
# Guarantees that a campaign can never be posted by two threads at once.
_AVP_ACTIVE = {}
_AVP_ACTIVE_LOCK = threading.Lock()


def _safe_extract_videos(zip_path, destination):
    """Extract supported video files safely. Returns (extracted_paths, skipped_names).

    Zip-slip is impossible (basename only + prefix assertion), unicode filenames
    are preserved, and empty/zero-byte entries are skipped instead of queued.
    """
    extracted, skipped = [], []
    os.makedirs(destination, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = os.path.basename(info.filename.replace('\\', '/'))
            ext = os.path.splitext(base)[1].lower()
            if ext not in ALLOWED_VIDEO_EXTENSIONS:
                continue
            candidate = ''.join(ch for ch in base if ch not in '\\/:*?"<>|').strip() or f'video{ext}'
            destination_abs = os.path.abspath(destination)
            target = os.path.abspath(os.path.join(destination_abs, candidate))
            if not target.startswith(destination_abs + os.sep):
                skipped.append(base)
                continue
            if os.path.exists(target):
                stem, ext2 = os.path.splitext(candidate)
                n = 2
                while os.path.exists(target):
                    target = os.path.abspath(os.path.join(destination_abs, f'{stem}_{n}{ext2}'))
                    n += 1
            with zf.open(info, 'r') as src, open(target, 'wb') as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            if os.path.getsize(target) == 0:
                try:
                    os.remove(target)
                except OSError:
                    pass
                skipped.append(base)
                continue
            extracted.append(target)
    return extracted, skipped


def _auto_caption(template, name, index, total, channel=None):
    """Render the caption template. Supported: {filename} {name} {index} {total} {channel}."""
    caption = template or ''
    base = os.path.basename(name or '')
    stem = os.path.splitext(base)[0]
    return (caption
            .replace('{filename}', base)
            .replace('{name}', stem)
            .replace('{index}', str(index + 1))
            .replace('{total}', str(total))
            .replace('{channel}', str(channel or '')))


def _parse_caption_pool(raw):
    """Split a caption pool into individual captions.

    Captions are separated by a line containing only dashes (---). A separator is
    used instead of "one caption per line" so that a single caption may still span
    several lines (the caption box has always allowed that).
    """
    text = (raw or '').replace('\r\n', '\n').replace('\r', '\n').strip()
    if not text:
        return []
    # The dashboard sends an explicit JSON array (one entry per caption box), which
    # is unambiguous; a plain string is also accepted and split on dash lines.
    if text.startswith('['):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return [str(p).strip() for p in loaded if str(p).strip()]
        except Exception:
            pass
    parts, current = [], []
    for line in text.split('\n'):
        stripped = line.strip()
        if len(stripped) >= 3 and set(stripped) == {'-'}:
            parts.append('\n'.join(current))
            current = []
        else:
            current.append(line)
    parts.append('\n'.join(current))
    return [p.strip() for p in parts if p and p.strip()]


def _campaign_captions(campaign):
    """The caption pool of a campaign, falling back to the single caption field."""
    pool = []
    try:
        loaded = json.loads(campaign.captions or '[]')
        if isinstance(loaded, list):
            pool = [str(p) for p in loaded if str(p).strip()]
    except Exception:
        pool = []
    if pool:
        return pool
    if (campaign.caption or '').strip():
        return [campaign.caption]
    return ['']


def _zip_videos(media_dir):
    """Video files still present on disk for a ZIP campaign."""
    try:
        names = sorted(os.listdir(media_dir))
    except OSError:
        return []
    result = []
    for name in names:
        path = os.path.join(media_dir, name)
        if os.path.splitext(name)[1].lower() not in ALLOWED_VIDEO_EXTENSIONS:
            continue
        try:
            if os.path.getsize(path) > 0:
                result.append(path)
        except OSError:
            continue
    return result


def _load_vault(campaign):
    """Telegram-side copies of posted videos: {filename: {'c': chat, 'm': msg_id}}.

    The first time a video is uploaded, the resulting Telegram message is
    remembered here. Later rounds (and every other channel) re-use that copy, so a
    wiped Railway disk can no longer orphan the library — and no video is uploaded
    twice.
    """
    try:
        data = json.loads(campaign.vault_state or '{}')
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    cleaned = {}
    for key, ref in data.items():
        if not isinstance(ref, dict):
            continue
        chat = clean_str(ref.get('c'))
        msg_id = safe_int(ref.get('m'), 0)
        if chat and msg_id > 0:
            cleaned[str(key)] = {'c': chat, 'm': msg_id}
    return cleaned


def _save_vault(campaign, vault):
    """Persist the Telegram-side copies in the database (survives restarts)."""
    try:
        campaign.vault_state = json.dumps({str(k): v for k, v in vault.items()})
        db.session.commit()
    except Exception as e:
        logger.warning('Could not persist auto-poster vault state: %s', e)


def _campaign_videos(campaign):
    """Return (video_refs, source_type, error_message_or_None).

    A ref is a local file path, 'saved:<msg_id>' (Telegram Saved Messages) or
    'vault:<filename>' (a Telegram-side copy of an uploaded video). The vault form
    is what keeps a ZIP campaign running after Railway wipes the ephemeral disk.
    """
    if campaign.source_type == 'saved_messages' or (campaign.source_type is None and campaign.source_ids):
        try:
            ids = [safe_int(x, 0) for x in json.loads(campaign.source_ids or '[]')]
        except Exception:
            ids = []
        ids = [i for i in ids if i > 0]
        if not ids:
            return [], 'saved_messages', 'Saved Messages source is empty — re-create the campaign.'
        return [f'saved:{i}' for i in ids], 'saved_messages', None
    videos = _zip_videos(campaign.media_dir)
    vault = _load_vault(campaign)
    if not videos and vault:
        # The local copies are gone (ephemeral Railway disk) but the videos
        # themselves are still on Telegram: keep posting from the durable copies
        # instead of dying with "No such file or directory".
        return ['vault:%s' % name for name in sorted(vault)], 'zip', None
    if not videos:
        return [], 'zip', ('Uploaded ZIP media is no longer on the server (the Railway filesystem is '
                           'ephemeral). Add the videos again from this card, or use Telegram Saved Messages.')
    return videos, 'zip', None


def _queue_state_path(media_dir):
    return os.path.join(media_dir, '.channel_queues.json')


def _build_queues(channels, total_videos):
    if not total_videos:
        return {'version': 3, 'channels': {}}
    return {
        'version': 3,
        'channels': {
            channel: {'order': random.sample(range(total_videos), total_videos), 'position': 0, 'cycle': 1}
            for channel in channels
        },
    }


def _load_queues(campaign, channels, total_videos):
    """Load per-channel queues from the DB, falling back to the legacy state file."""
    state = None
    if campaign.queue_state:
        try:
            parsed = json.loads(campaign.queue_state)
            if parsed.get('version') == 3:
                state = parsed
        except Exception:
            state = None
    if state is None:
        try:
            with open(_queue_state_path(campaign.media_dir), 'r', encoding='utf-8') as fh:
                parsed = json.load(fh)
            if parsed.get('version') in (2, 3):
                parsed['version'] = 3
                if parsed['channels'] and total_videos:
                    for item in parsed['channels'].values():
                        if sorted(item.get('order', [])) != list(range(total_videos)):
                            item['order'] = random.sample(range(total_videos), total_videos)
                            item['position'] = 0
                state = parsed
        except Exception:
            state = None
    if state is None:
        state = _build_queues(channels, total_videos)

    changed = False
    for channel in channels:
        item = state.setdefault('channels', {}).get(channel)
        if not item or sorted(item.get('order', [])) != list(range(total_videos)):
            state['channels'][channel] = {
                'order': random.sample(range(total_videos), total_videos),
                'position': 0, 'cycle': 1,
            } if total_videos else {'order': [], 'position': 0, 'cycle': 1}
            changed = True
    for stale in [c for c in list(state.get('channels', {})) if c not in channels]:
        state['channels'].pop(stale, None)
        changed = True
    if changed:
        _save_queues(campaign, state)
    return state


def _save_queues(campaign, state):
    """Persist queue positions in the database (durable) and mirror them to disk."""
    try:
        campaign.queue_state = json.dumps(state)
        db.session.commit()
    except Exception as e:
        logger.warning('Could not persist auto-poster queue state: %s', e)
    try:
        with open(_queue_state_path(campaign.media_dir), 'w', encoding='utf-8') as fh:
            json.dump(state, fh)
    except OSError:
        pass


@app.route('/bot/auto_video_post/create', methods=['POST'])
@login_required
def auto_video_post_create():
    """Create an auto-poster from uploaded ZIP files or from Telegram Saved Messages."""
    media_dir = None
    try:
        account_id = safe_int(request.form.get('account_id'), 0)
        source_type = (clean_str(request.form.get('source_type'), 'zip') or 'zip').lower()
        channels_raw = clean_str(request.form.get('channels'))
        caption = clean_str(request.form.get('caption'))
        # Multiple captions: one per "---" separated block. A random one is picked
        # per round; if none are given we fall back to the single caption field.
        caption_pool = _parse_caption_pool(request.form.get('captions'))
        if not caption_pool and caption:
            caption_pool = [caption]
        min_delay = safe_int(request.form.get('min_delay'), 5, minimum=AUTO_POST_DELAY_MIN, maximum=AUTO_POST_DELAY_MAX)
        max_delay = safe_int(request.form.get('max_delay'), 15, minimum=min_delay, maximum=AUTO_POST_DELAY_MAX)

        if not account_id:
            return jsonify({'success': False, 'error': 'Select a Telegram account first'}), 400
        if not channels_raw:
            return jsonify({'success': False, 'error': 'Add at least one channel'}), 400

        channels, rejected = [], []
        for raw in channels_raw.splitlines():
            raw = raw.strip()
            if not raw:
                continue
            if is_protected_target(raw):
                return jsonify({'success': False, 'error': f'Protected channel cannot be targeted: {raw}'}), 400
            parsed = parse_channel_target(raw)
            if not parsed:
                rejected.append(raw)
                continue
            normalised = raw.strip()
            if parsed['kind'] in ('username', 'id'):
                normalised = parsed['value']
            if normalised not in channels:
                channels.append(normalised)
        if rejected:
            return jsonify({
                'success': False,
                'error': 'Invalid channel identifier(s): ' + ', '.join(rejected[:5]),
            }), 400
        if not channels:
            return jsonify({'success': False, 'error': 'No valid channels'}), 400
        # Shared ceiling: identical to what the dashboard displays via /bot/app_config.
        # A falsy value means the operator chose "no limit".
        if MAX_AUTO_POST_CHANNELS and len(channels) > MAX_AUTO_POST_CHANNELS:
            return jsonify({
                'success': False,
                'error': f'Maximum {MAX_AUTO_POST_CHANNELS} channels per campaign',
            }), 400
        if source_type not in ('zip', 'saved_messages'):
            return jsonify({'success': False, 'error': 'Invalid video source'}), 400

        acc = get_account_for_current_user(account_id)
        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Connected Telegram account not found'}), 400
        api_id, api_hash = get_api_credentials(acc.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API ID/API Hash is not configured'}), 400

        campaign_uid = uuid.uuid4().hex
        media_dir = os.path.join(AUTO_VIDEO_ROOT, campaign_uid)
        os.makedirs(media_dir, exist_ok=True)
        all_videos, source_ids = [], []
        skipped_files = []

        if source_type == 'zip':
            files = request.files.getlist('zip_files')
            if not files or not any(f and f.filename for f in files):
                shutil.rmtree(media_dir, ignore_errors=True)
                return jsonify({'success': False, 'error': 'Upload one or more ZIP files'}), 400
            temp_zips = []
            for stored in files:
                if not stored or not stored.filename or not stored.filename.lower().endswith('.zip'):
                    continue
                tmp = os.path.join(media_dir, '__' + secure_filename(stored.filename))
                stored.save(tmp)
                temp_zips.append(tmp)
                try:
                    found, skipped = _safe_extract_videos(tmp, media_dir)
                except zipfile.BadZipFile:
                    shutil.rmtree(media_dir, ignore_errors=True)
                    return jsonify({'success': False, 'error': f'{stored.filename} is not a valid ZIP file'}), 400
                all_videos.extend(found)
                skipped_files.extend(skipped)
            for tmp in temp_zips:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            all_videos.sort(key=lambda path: os.path.basename(path).lower())
            if not all_videos:
                shutil.rmtree(media_dir, ignore_errors=True)
                return jsonify({
                    'success': False,
                    'error': 'No supported video files found in the uploaded ZIP file(s)',
                }), 400
        else:
            # Telegram Saved Messages: persist only the message IDs, never the videos.
            proxy = get_proxy_for_account(acc.user_id, account_id)
            sess = acc.session_string

            async def collect_saved_video_ids():
                client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                await client.connect()
                try:
                    ids = []
                    scanned = 0
                    async for msg in client.iter_messages('me', limit=SAVED_MESSAGES_SCAN_LIMIT):
                        scanned += 1
                        mime = ''
                        file_obj = getattr(msg, 'file', None)
                        if file_obj is not None:
                            mime = getattr(file_obj, 'mime_type', '') or ''
                        if getattr(msg, 'video', None) or mime.startswith('video/'):
                            ids.append(safe_int(msg.id, 0))
                    return [i for i in ids if i > 0], scanned
                finally:
                    await client.disconnect()

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                source_ids, scanned = loop.run_until_complete(collect_saved_video_ids())
            except FloodWaitError as e:
                shutil.rmtree(media_dir, ignore_errors=True)
                return jsonify({
                    'success': False,
                    'error': f'Telegram rate limit while scanning Saved Messages — retry after '
                             f'{safe_int(getattr(e, "seconds", 0), 5)}s',
                }), 429
            finally:
                loop.close()
            if not source_ids:
                shutil.rmtree(media_dir, ignore_errors=True)
                return jsonify({
                    'success': False,
                    'error': f'No video messages found in Saved Messages (scanned {scanned} messages)',
                }), 400
            all_videos = [f'saved:{mid}' for mid in source_ids]

        campaign = AutoVideoPostCampaign(
            user_id=acc.user_id,
            account_id=account_id,
            channels=json.dumps(channels),
            media_dir=media_dir,
            caption=caption,
            captions=json.dumps(caption_pool),
            min_delay=min_delay,
            max_delay=max_delay,
            current_index=0,
            total_videos=len(all_videos),
            status='running',
            next_run_at=datetime.utcnow(),
            source_type=source_type,
            source_ids=json.dumps(source_ids),
            posted_count=0, failed_count=0, skipped_count=0, rounds_completed=0,
            stop_requested=False, needs_reupload=False,
            worker_heartbeat=datetime.utcnow(),
        )
        db.session.add(campaign)
        db.session.commit()
        _save_queues(campaign, _build_queues(channels, len(all_videos)))

        source_label = 'Telegram Saved Messages' if source_type == 'saved_messages' else 'ZIP files'
        log_activity(
            acc.user_id,
            f'Auto Video Poster created from {source_label}: {len(all_videos)} videos → '
            f'{len(channels)} channels',
            'success', account_id,
        )
        payload = {
            'success': True,
            'campaign_id': campaign.id,
            'videos': len(all_videos),
            'channels': len(channels),
            'source': 'Saved Messages' if source_type == 'saved_messages' else 'ZIP files',
            'max_channels': MAX_AUTO_POST_CHANNELS,
        }
        if skipped_files:
            payload['skipped_files'] = skipped_files[:10]
        return jsonify(payload)

    except RequestEntityTooLarge:
        return jsonify({
            'success': False,
            'error': f'Upload is larger than the {MAX_UPLOAD_MB} MB limit. Split the ZIP files or raise MAX_UPLOAD_MB.',
        }), 413
    except Exception as e:
        if media_dir:
            shutil.rmtree(media_dir, ignore_errors=True)
        logger.exception('auto_video_post_create failed')
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


@app.route('/bot/auto_video_post/list')
@login_required
def auto_video_post_list():
    campaigns = (AutoVideoPostCampaign.query
                 .filter_by(user_id=current_user.id)
                 .order_by(AutoVideoPostCampaign.created_at.desc())
                 .limit(MAX_LIST_ROWS).all())
    result = []
    for c in campaigns:
        # Always resolve the library: a wiped disk is no longer a dead end because
        # the Telegram-side copies keep the campaign usable.
        videos, source_type, source_error = _campaign_videos(c)
        channels = []
        try:
            channels = json.loads(c.channels or '[]')
        except Exception:
            channels = []
        result.append({
            'id': c.id,
            'channels': channels,
            'channel_count': len(channels),
            'current_index': c.current_index or 0,
            'total_videos': c.total_videos or 0,
            'status': c.status,
            'source': 'Saved Messages' if (c.source_type == 'saved_messages') else 'ZIP files',
            'source_type': c.source_type or 'zip',
            'min_delay': c.min_delay, 'max_delay': c.max_delay,
            'captions': _campaign_captions(c),
            'caption_pool_size': len(_campaign_captions(c)),
            'next_run_at': c.next_run_at.strftime('%Y-%m-%d %H:%M:%S') if c.next_run_at else None,
            'last_post_at': c.last_post_at.strftime('%Y-%m-%d %H:%M:%S') if c.last_post_at else None,
            'last_error': c.last_error or '',
            # Real, confirmed numbers — this is what the dashboard shows.
            'posted_count': c.posted_count or 0,
            'failed_count': c.failed_count or 0,
            'skipped_count': c.skipped_count or 0,
            'rounds_completed': c.rounds_completed or 0,
            'available_videos': len(videos),
            'vault_count': len(_load_vault(c)),
            'needs_reupload': bool(c.needs_reupload) and not _load_vault(c),
            'last_round_summary': c.last_round_summary or '',
            'worker_alive': bool(c.worker_heartbeat and (datetime.utcnow() - c.worker_heartbeat).total_seconds() < 900),
        })
    return jsonify({'success': True, 'campaigns': result, 'max_channels': MAX_AUTO_POST_CHANNELS})


@app.route('/bot/auto_video_post/<int:campaign_id>/pause', methods=['POST'])
@login_required
def auto_video_post_pause(campaign_id):
    c = AutoVideoPostCampaign.query.filter_by(id=campaign_id, user_id=current_user.id).first()
    if not c:
        return jsonify({'success': False, 'error': 'Campaign not found'}), 404
    c.status = 'paused'
    db.session.commit()
    log_activity(current_user.id, f'Auto Video Poster campaign {campaign_id} paused', 'success', c.account_id)
    return jsonify({'success': True, 'message': 'Campaign paused'})


@app.route('/bot/auto_video_post/<int:campaign_id>/resume', methods=['POST'])
@login_required
def auto_video_post_resume(campaign_id):
    c = AutoVideoPostCampaign.query.filter_by(id=campaign_id, user_id=current_user.id).first()
    if not c:
        return jsonify({'success': False, 'error': 'Campaign not found'}), 404
    videos, source_type, source_error = _campaign_videos(c)
    if not videos:
        c.status = 'paused'
        c.needs_reupload = True if source_type == 'zip' else c.needs_reupload
        c.last_error = source_error
        db.session.commit()
        return jsonify({'success': False, 'error': source_error}), 400
    c.status = 'running'
    c.next_run_at = datetime.utcnow()
    c.stop_requested = False
    c.needs_reupload = False
    c.last_error = None
    c.worker_heartbeat = datetime.utcnow()
    db.session.commit()
    log_activity(current_user.id, f'Auto Video Poster campaign {campaign_id} resumed', 'success', c.account_id)
    return jsonify({'success': True, 'message': 'Campaign resumed'})


@app.route('/bot/auto_video_post/<int:campaign_id>/add_videos', methods=['POST'])
@login_required
def auto_video_post_add_videos(campaign_id):
    """Merge more ZIP files into an existing campaign without losing anything.

    This is the supported way to feed a very large library in (50k+ videos): the
    channels, caption, delays, confirmed counters and Telegram-side copies all stay
    exactly as they are, and the per-channel queues are rebuilt over the enlarged
    library. It also revives a campaign whose uploaded media was wiped by a
    redeploy, without having to re-create it.
    """
    c = AutoVideoPostCampaign.query.filter_by(id=campaign_id, user_id=current_user.id).first()
    if not c:
        return jsonify({'success': False, 'error': 'Campaign not found'}), 404
    files = request.files.getlist('zip_files')
    if not files or not any(f and f.filename for f in files):
        return jsonify({'success': False, 'error': 'Upload one or more ZIP files'}), 400
    try:
        os.makedirs(c.media_dir, exist_ok=True)
    except OSError as e:
        return jsonify({'success': False, 'error': f'The media folder is not writable: {e}'}), 500

    added, skipped_files = [], []
    for stored in files:
        if not stored or not stored.filename or not stored.filename.lower().endswith('.zip'):
            continue
        tmp = os.path.join(c.media_dir, '__' + secure_filename(stored.filename))
        try:
            stored.save(tmp)
            found, skipped = _safe_extract_videos(tmp, c.media_dir)
        except zipfile.BadZipFile:
            return jsonify({'success': False, 'error': f'{stored.filename} is not a valid ZIP file'}), 400
        except OSError as e:
            return jsonify({'success': False, 'error': f'Could not store {stored.filename}: {e}'}), 500
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        added.extend(found)
        skipped_files.extend(skipped)

    if not added:
        return jsonify({'success': False,
                        'error': 'No supported video files found in the uploaded ZIP file(s)'}), 400

    videos, source_type, source_error = _campaign_videos(c)
    c.total_videos = len(videos)
    c.needs_reupload = False
    c.last_error = None
    c.status = 'running'
    c.stop_requested = False
    c.next_run_at = datetime.utcnow()
    c.worker_heartbeat = datetime.utcnow()
    db.session.commit()
    try:
        channels = json.loads(c.channels or '[]')
    except Exception:
        channels = []
    # The library grew, so every channel queue is rebuilt over the new total.
    _save_queues(c, _build_queues(channels, len(videos)))
    log_activity(
        c.user_id,
        f'Auto Video Poster campaign {campaign_id}: {len(added)} more video(s) added '
        f'({len(videos)} now in the library)',
        'success', c.account_id,
    )
    return jsonify({
        'success': True, 'added': len(added), 'skipped': len(skipped_files),
        'videos': len(videos),
        'message': f'{len(added)} video(s) added — {len(videos)} now in the library',
    })


@app.route('/bot/auto_video_post/<int:campaign_id>', methods=['DELETE'])
@login_required
def auto_video_post_delete(campaign_id):
    c = AutoVideoPostCampaign.query.filter_by(id=campaign_id, user_id=current_user.id).first()
    if not c:
        return jsonify({'success': False, 'error': 'Campaign not found'}), 404
    # Ask a running round to stop before removing the media it is reading.
    if c.status == 'posting':
        c.stop_requested = True
        db.session.commit()
        for _ in range(10):
            time.sleep(0.5)
            db.session.refresh(c)
            if c.status != 'posting':
                break
    media_dir = c.media_dir
    account_id = c.account_id
    db.session.delete(c)
    db.session.commit()
    shutil.rmtree(media_dir, ignore_errors=True)
    log_activity(current_user.id, f'Auto Video Poster campaign {campaign_id} deleted', 'success', account_id)
    return jsonify({'success': True, 'message': 'Campaign deleted'})


async def _resolve_channel_entity(client, channel):
    """Resolve a channel reference (username, numeric id, invite link, private link)."""
    info = parse_channel_target(channel)
    if not info:
        raise RuntimeError(f'invalid channel reference: {channel}')
    if info['kind'] == 'invite':
        from telethon.tl.functions.messages import ImportChatInviteRequest
        invite_hash = info['value'].lstrip('+')
        try:
            updates = await client(ImportChatInviteRequest(invite_hash))
            if getattr(updates, 'chats', None):
                return updates.chats[0]
        except UserAlreadyParticipantError:
            pass
        except FloodWaitError:
            raise
        except Exception as e:
            raise RuntimeError(f'invite link could not be used ({e})')
        # Already a member: find it in the current dialogs.
        async for dialog in client.iter_dialogs():
            if invite_hash and invite_hash.lower() in str(getattr(dialog.entity, 'username', '') or '').lower():
                return dialog.entity
        raise RuntimeError('invite link: already joined but the chat could not be resolved')
    if info['kind'] == 'private_post':
        try:
            return await client.get_entity(types.PeerChannel(int(info['value'])))
        except Exception as e:
            raise RuntimeError(f'private channel link could not be resolved ({e})')
    return await client.get_entity(safe_int(info['value'], 0) or info['value'])


def _run_auto_post_round(campaign_id):
    """Run one posting round, guaranteeing at most one round per campaign.

    The scheduler re-arms a 'posting' campaign once its heartbeat looks stale.
    A round that is legitimately slow looked stale by that rule, so a second
    thread posted the same video to the same channel. The in-process registry
    below makes that impossible while the round is alive.
    """
    with _AVP_ACTIVE_LOCK:
        if campaign_id in _AVP_ACTIVE:
            logger.warning(
                'Auto Video Poster: a round for campaign %s is already running in this '
                'process — not starting a second one', campaign_id,
            )
            return
        _AVP_ACTIVE[campaign_id] = threading.current_thread()
    try:
        _run_auto_post_round_body(campaign_id)
    except Exception:
        # The body records its own failures; this only catches something that
        # escaped it. The thread must not die silently, and the registry entry
        # must go in every case.
        logger.exception('auto_video_post round crashed for campaign %s', campaign_id)
    finally:
        with _AVP_ACTIVE_LOCK:
            _AVP_ACTIVE.pop(campaign_id, None)


def _avp_active_count():
    """How many rounds this process is currently running."""
    with _AVP_ACTIVE_LOCK:
        return len(_AVP_ACTIVE)


def _run_auto_post_round_body(campaign_id):
    """Execute a single posting round for one campaign (called in its own thread)."""
    with app.app_context():
        campaign = AutoVideoPostCampaign.query.get(campaign_id)
        if not campaign:
            return
        # When this round STARTED. The inter-round interval is measured from here,
        # not from when the round finishes (see the scheduling block at the end).
        round_started = datetime.utcnow()
        # Self-heal the campaign folder. It is created when the campaign is created,
        # so on an ephemeral Railway disk it disappears on every redeploy while the
        # campaign row survives in PostgreSQL. Any later os.listdir/open on that
        # path then died with:
        #   [Errno 2] No such file or directory: '/data/auto_post_media/<uid>'
        # which failed the whole round (0 posted) and parked the error on the card.
        if campaign.media_dir:
            try:
                os.makedirs(campaign.media_dir, exist_ok=True)
            except OSError as exc:
                logger.warning('auto_video_post: could not create media dir %s: %s',
                               campaign.media_dir, exc)
        try:
            acc = TelegramAccount.query.get(campaign.account_id)
            api_id, api_hash = get_api_credentials(campaign.user_id)
            if not acc or not acc.session_string or not api_id:
                raise RuntimeError('Telegram account or API credentials are unavailable')

            channels = json.loads(campaign.channels or '[]')
            if not channels:
                raise RuntimeError('campaign has no channels')

            videos, source_type, source_error = _campaign_videos(campaign)
            if not videos:
                campaign.status = 'paused'
                campaign.needs_reupload = (source_type == 'zip')
                campaign.last_error = source_error
                db.session.commit()
                return

            # Telegram-side copies of already-posted videos (durable library).
            vault = _load_vault(campaign)
            if vault and campaign.needs_reupload:
                # The library is reachable again through Telegram.
                campaign.needs_reupload = False
                db.session.commit()
            state = _load_queues(campaign, channels, len(videos))
            selected, used = {}, set()
            for channel in channels:
                queue = state['channels'][channel]
                if queue['position'] >= len(queue['order']):
                    queue['order'] = random.sample(range(len(videos)), len(videos))
                    queue['position'] = 0
                    queue['cycle'] = safe_int(queue.get('cycle'), 1) + 1
                candidate = queue['order'][queue['position']]
                if len(videos) >= len(channels) and candidate in used:
                    alternatives = [i for i in queue['order'][queue['position']:] if i not in used]
                    if alternatives:
                        candidate = alternatives[0]
                        queue['order'].remove(candidate)
                        queue['order'].insert(queue['position'], candidate)
                selected[channel] = candidate
                used.add(candidate)

            proxy = get_proxy_for_account(campaign.user_id, campaign.account_id)
            sess = acc.session_string
            counters = {'posted': 0, 'failed': 0, 'skipped': 0}
            errors = []
            # filename -> Telegram message already posted during THIS round. The
            # file is uploaded once and the remaining channels re-send that copy.
            round_cache = {}
            library_total = safe_int(campaign.total_videos, 0) or len(videos)

            async def send_round():
                client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                await client.connect()
                # Pick ONE caption for this whole round, before the channel loop, so
                # every channel in the round posts the same caption text (and the
                # choice is not re-rolled per channel).
                _pool = _campaign_captions(campaign)
                round_caption = random.choice(_pool) if _pool else ''
                try:
                    for channel in channels:
                        # Stop promptly when the operator paused/deleted the campaign.
                        db.session.refresh(campaign)
                        if campaign.stop_requested or campaign.status not in ('posting', 'running'):
                            break
                        index = selected[channel]
                        queue = state['channels'][channel]
                        attempts = 0
                        while attempts < 2:
                            attempts += 1
                            try:
                                ref = str(videos[index])
                                source_msg = None
                                payload = None
                                if source_type == 'saved_messages':
                                    msg_id = safe_int(ref.split(':', 1)[1], 0)
                                    source_msg = await client.get_messages('me', ids=msg_id)
                                    if not source_msg or not getattr(source_msg, 'media', None):
                                        # This Saved Message is gone: skip just this video.
                                        counters['skipped'] += 1
                                        errors.append(f'{channel}: Saved Message {msg_id} is no longer available')
                                        queue['position'] += 1
                                        break
                                    file_obj = getattr(source_msg, 'file', None)
                                    name = getattr(file_obj, 'name', None) or f'video_{msg_id}'
                                    payload = source_msg.media
                                else:
                                    name = ref.split(':', 1)[1] if ref.startswith('vault:') \
                                        else os.path.basename(ref)
                                    # Re-use the Telegram copy: (1) posted earlier in this round,
                                    # (2) stored in a previous round (survives a wiped disk).
                                    source_msg = round_cache.get(name)
                                    if source_msg is None:
                                        stored = vault.get(name)
                                        if stored:
                                            try:
                                                peer = await _resolve_channel_entity(client, stored['c'])
                                                fetched = await client.get_messages(peer, ids=stored['m'])
                                            except Exception:
                                                fetched = None
                                            if fetched is not None and getattr(fetched, 'media', None):
                                                source_msg = fetched
                                            else:
                                                vault.pop(name, None)
                                    if source_msg is None and not os.path.exists(ref):
                                        counters['skipped'] += 1
                                        errors.append(
                                            f'{channel}: no reachable copy of {name} (the server copy was '
                                            f'wiped and no Telegram copy exists yet)'
                                        )
                                        queue['position'] += 1
                                        break
                                    payload = source_msg if source_msg is not None else ref

                                # Hard guard before handing anything to Telegram. A wiped
                                # ephemeral disk (Railway /data without a volume) used to surface
                                # here as
                                #   [Errno 2] No such file or directory: '/data/auto_post_media/<uid>'
                                # and it failed the ENTIRE round, so nothing was posted and the
                                # operator saw "0 posted" plus a confusing filesystem error. A path
                                # that is missing (or is a folder) is now a clean per-channel skip:
                                # the round keeps going and the reason is stated in plain words.
                                if payload is None or (
                                    isinstance(payload, str)
                                    and (os.path.isdir(payload) or not os.path.exists(payload))
                                ):
                                    counters['skipped'] += 1
                                    errors.append(
                                        f'{channel}: media for {name} is not on the server and has no '
                                        f'Telegram copy yet — skipped'
                                    )
                                    queue['position'] += 1
                                    break

                                entity = await _resolve_channel_entity(client, channel)
                                caption = _auto_caption(round_caption, name, index, library_total, channel)
                                sent = await client.send_file(entity, payload, caption=caption or None)
                                if not sent:
                                    raise RuntimeError('Telegram did not confirm the post')
                                if source_type != 'saved_messages' and source_msg is None:
                                    # First upload of this video: remember the Telegram copy so a
                                    # redeploy can never orphan it, and re-use it for the other
                                    # channels instead of uploading the same file again.
                                    round_cache[name] = sent
                                    vault[name] = {'c': channel, 'm': safe_int(getattr(sent, 'id', 0), 0)}
                                # Confirmed by Telegram: only now advance the queue.
                                counters['posted'] += 1
                                queue['position'] += 1
                                campaign.current_index = max(
                                    q['position'] for q in state['channels'].values()
                                )
                                campaign.worker_heartbeat = datetime.utcnow()
                                campaign.last_post_at = datetime.utcnow()
                                _save_queues(campaign, state)
                                if source_type != 'saved_messages':
                                    _save_vault(campaign, vault)
                                break
                            except FloodWaitError as e:
                                # Honour the wait, then retry this one channel once.
                                wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
                                errors.append(f'{channel}: rate limit — waited {wait}s')
                                await asyncio.sleep(wait)
                                if attempts >= 2:
                                    counters['skipped'] += 1
                                    break
                            except Exception as e:
                                counters['failed'] += 1
                                errors.append(friendly_telegram_error(e, channel))
                                break
                finally:
                    await client.disconnect()
                return counters

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                counters = loop.run_until_complete(send_round())
            finally:
                loop.close()

            campaign = AutoVideoPostCampaign.query.get(campaign_id)
            if not campaign:
                return
            _save_queues(campaign, state)
            campaign.posted_count = (campaign.posted_count or 0) + counters['posted']
            campaign.failed_count = (campaign.failed_count or 0) + counters['failed']
            campaign.skipped_count = (campaign.skipped_count or 0) + counters['skipped']
            campaign.rounds_completed = (campaign.rounds_completed or 0) + 1
            campaign.last_round_summary = (
                f"posted {counters['posted']}, failed {counters['failed']}, "
                f"skipped {counters['skipped']} of {len(channels)} channels"
            )
            campaign.last_error = '\n'.join(errors[-10:]) if errors else None
            campaign.worker_heartbeat = datetime.utcnow()
            # Persist the real counters first, then re-read the row: the operator may
            # have pressed Pause (a different request/session) while this round was
            # running. Previously the tail wrote 'running' unconditionally, so Pause
            # appeared to work and then silently reverted itself.
            db.session.commit()
            try:
                db.session.refresh(campaign)
            except Exception:
                pass
            if campaign.stop_requested or campaign.status == 'paused':
                campaign.stop_requested = False
                campaign.status = 'paused'
                campaign.next_run_at = None      # nothing may be scheduled while paused
            else:
                if campaign.status == 'posting':
                    campaign.status = 'running'
                low = max(1, campaign.min_delay) * 60
                high = max(low, max(1, campaign.max_delay) * 60)
                delay_seconds = random.randint(low, high)
                if not counters['posted']:
                    # Nothing got through (rate limit / permissions / wiped library).
                    # Wait at least 2 minutes so we do not hammer a rate-limited
                    # account, but never longer than the operator's own range: the
                    # old code doubled the delay and floored it at 5-15 minutes,
                    # which silently overrode a "3 minutes" setting. A round that
                    # hits a real flood wait already sleeps inside the round loop.
                    delay_seconds = max(delay_seconds, 120)
                    logger.info(
                        'auto_video_post campaign %s: round posted nothing (%s), '
                        'waiting %ss (operator range %ss-%ss)',
                        campaign_id, campaign.last_round_summary, delay_seconds,
                        max(1, campaign.min_delay) * 60, max(1, campaign.max_delay) * 60,
                    )
                # The interval is start-to-start. A round posts one video per
                # channel, so it can take many minutes by itself; adding the whole
                # delay on top of the round's own duration is what made a "3 minute"
                # setting behave like 10-20 minutes.
                target = round_started + timedelta(seconds=delay_seconds)
                now = datetime.utcnow()
                if target <= now:
                    # The round overran its own interval: go again after a short
                    # guard instead of waiting one more full period.
                    campaign.next_run_at = now + timedelta(seconds=5)
                    logger.info(
                        'auto_video_post campaign %s: round took longer than the %ss '
                        'interval, re-arming in 5s',
                        campaign_id, delay_seconds,
                    )
                else:
                    campaign.next_run_at = target
            db.session.commit()
            # "campaign started" is never reported as success — these are real counts.
            log_activity(
                campaign.user_id,
                f"Auto Video Poster round ({len(channels)} channels): {campaign.last_round_summary}",
                'success' if counters['posted'] else 'failed',
                campaign.account_id,
            )
        except Exception as e:
            campaign = AutoVideoPostCampaign.query.get(campaign_id)
            if campaign is not None:
                # Re-read the row: the operator may have paused or deleted the
                # campaign (from another request, i.e. another session) while this
                # round was running. A stale in-session copy would otherwise
                # resurrect a campaign the operator had just switched off.
                try:
                    db.session.refresh(campaign)
                except Exception:
                    campaign = None
            if campaign is not None:
                if campaign.status in ('posting', 'running'):
                    campaign.status = 'running'
                    campaign.next_run_at = datetime.utcnow() + timedelta(minutes=5)
                campaign.last_error = friendly_telegram_error(e)
                campaign.worker_heartbeat = datetime.utcnow()
                db.session.commit()
            logger.exception('auto_video_post round failed for campaign %s', campaign_id)


def run_auto_video_post_scheduler():
    """Background scheduler: one posting round per campaign, then reschedule.

    Each channel has an independent shuffled queue, so a channel never repeats a
    video until it has used the whole library, and channels get different videos
    within a round whenever the library is large enough.
    """
    def worker():
        while True:
            time.sleep(10)
            try:
                with app.app_context():
                    now = datetime.utcnow()
                    # Recover rounds that were killed mid-flight in an earlier process.
                    # A round has no long waits inside it, so 5 minutes of silence
                    # always means the worker is gone — re-arm it (previously a
                    # crashed round stalled the campaign until the next redeploy).
                    stale_cutoff = now - timedelta(minutes=5)
                    for stuck in AutoVideoPostCampaign.query.filter(
                        AutoVideoPostCampaign.status == 'posting'
                    ).all():
                        # A round owned by a LIVE thread in this process is never
                        # "recovered" — doing so started a duplicate round, which
                        # posted the same video to the same channel twice.
                        with _AVP_ACTIVE_LOCK:
                            still_alive = stuck.id in _AVP_ACTIVE
                        if still_alive:
                            continue
                        if not stuck.worker_heartbeat or stuck.worker_heartbeat < stale_cutoff:
                            logger.warning(
                                'Auto Video Poster: campaign %s was stuck in "posting" with no '
                                'worker — re-arming it', stuck.id,
                            )
                            stuck.status = 'running'
                            stuck.worker_heartbeat = now
                    db.session.commit()

                    campaigns = AutoVideoPostCampaign.query.filter(
                        AutoVideoPostCampaign.status == 'running',
                        AutoVideoPostCampaign.next_run_at <= now
                    ).limit(MAX_SCHEDULED_BATCH).all()
                    for c in campaigns:
                        with _AVP_ACTIVE_LOCK:
                            already_running = c.id in _AVP_ACTIVE
                        if already_running:
                            continue
                        # Honour the operator's concurrency ceiling.
                        if AUTO_POST_MAX_CONCURRENT and _avp_active_count() >= AUTO_POST_MAX_CONCURRENT:
                            logger.info(
                                'Auto Video Poster: %s concurrent rounds already running — '
                                'campaign %s waits for the next tick',
                                AUTO_POST_MAX_CONCURRENT, c.id,
                            )
                            break
                        c.status = 'posting'
                        c.worker_heartbeat = now
                        db.session.commit()
                        campaign_id = c.id
                        threading.Thread(
                            target=_run_auto_post_round, args=(campaign_id,),
                            daemon=True, name=f'avp-round-{campaign_id}',
                        ).start()
            except Exception:
                logger.exception('auto_video_post scheduler loop failed')
    threading.Thread(target=worker, daemon=True, name='avp-scheduler').start()


# ═══════════════════════════════════════════════════════════════════════════════
# ─── SAVED TARGETS + APP CONFIG + DATABASE HEALTH ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

async def _deliver_media_or_text(client, peer, text, media_path=None):
    """Send plain text, or a device-uploaded photo/video using the text as caption.

    Used by every messaging tool (personal chat, bulk DM, DM campaign, channel
    post) so a single upload path covers them all. Existing text-only behaviour
    is unchanged when media_path is empty.
    """
    media_path = (media_path or '').strip()
    if media_path:
        if not os.path.exists(media_path):
            raise FileNotFoundError(
                'The uploaded photo/video is no longer on the server (temporary disk). '
                'Re-upload it and try again.')
        return await client.send_file(peer, media_path, caption=(text or None))
    return await client.send_message(peer, text)


@app.route('/bot/upload_media', methods=['POST'])
@login_required
def upload_media():
    """Accept a photo or video straight from the operator's device.

    The dashboard posts one or more files here, gets back the stored absolute
    path and drops it into the existing "media URL/path" field, so Broadcast,
    Scheduled Broadcast, Group Message and Channel Post can all send it without
    the operator needing a public URL.
    """
    files = request.files.getlist('file') or request.files.getlist('files')
    files = [f for f in files if f and clean_str(f.filename)]
    if not files:
        return jsonify({'success': False, 'error': 'No file received'}), 400

    saved = []
    for stored in files:
        original = clean_str(stored.filename)
        ext = os.path.splitext(original)[1].lower()
        if ext not in ALLOWED_MEDIA_EXTENSIONS:
            return jsonify({
                'success': False,
                'error': (f"'{original}' is not a supported media type. "
                          "Photos: jpg, jpeg, png, webp, gif. Videos: mp4, mov, mkv, webm, avi, m4v."),
            }), 400
        safe = secure_filename(original) or 'upload'
        name = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}_{safe}"
        target = os.path.join(MESSAGING_MEDIA_ROOT, name)
        try:
            stored.save(target)
        except Exception as e:
            logger.error('Media upload failed: %s', e)
            return jsonify({'success': False, 'error': f'Could not store {original}: {e}'}), 500
        size = os.path.getsize(target) if os.path.exists(target) else 0
        if size == 0:
            try:
                os.remove(target)
            except OSError:
                pass
            return jsonify({'success': False, 'error': f"'{original}' is empty"}), 400
        saved.append({
            'name': name,
            'original_name': original,
            'path': target,
            'url': url_for('messaging_media', filename=name),
            'kind': 'photo' if ext in ALLOWED_IMAGE_EXTENSIONS else 'video',
            'size': size,
            'persistent': MESSAGING_MEDIA_ROOT.startswith(os.path.join(os.sep, 'data') + os.sep)
                          or bool(os.environ.get('MESSAGING_MEDIA_DIR')),
        })

    first = saved[0]
    log_activity(current_user.id,
                 f"Uploaded {first['kind']} from device: {first['original_name']} "
                 f"({round(first['size'] / 1048576, 2)} MB)",
                 'success')
    return jsonify({'success': True, 'file': first, 'files': saved})


@app.route('/media/<path:filename>')
@login_required
def messaging_media(filename):
    """Serve an uploaded photo/video back to the dashboard so it can preview it."""
    try:
        return send_from_directory(MESSAGING_MEDIA_ROOT, filename)
    except Exception:
        return jsonify({'success': False, 'error': 'File not found'}), 404


@app.route('/bot/app_config')
@login_required
def app_config():
    """Runtime limits shared with the frontend (keeps UI text and validation in sync)."""
    return jsonify({
        'success': True,
        'max_auto_post_channels': MAX_AUTO_POST_CHANNELS,
        'auto_post_delay_min': AUTO_POST_DELAY_MIN,
        'auto_post_delay_max': AUTO_POST_DELAY_MAX,
        'max_upload_mb': MAX_UPLOAD_MB,
        'max_delay_seconds': MAX_DELAY_SECONDS,
        'max_delay_minutes': AUTO_POST_DELAY_MAX,
        'max_repeat_count': MAX_REPEAT_COUNT,
        'max_forward_messages': MAX_FORWARD_MESSAGES,
        'max_history_limit': MAX_HISTORY_LIMIT,
        'max_bulk_count': MAX_BULK_COUNT,
        'max_scrape_limit': MAX_SCRAPE_LIMIT,
        'upload_unlimited': MAX_UPLOAD_MB == 0,
        'registration_enabled': ALLOW_REGISTRATION,
        'background_jobs': BACKGROUND_JOBS_ENABLED,
        'database': 'postgresql' if DB_IS_POSTGRES else 'sqlite',
    })


@app.route('/bot/db_health')
@login_required
def db_health():
    """Report database connectivity without exposing credentials."""
    info = {
        'success': True,
        'backend': 'postgresql' if DB_IS_POSTGRES else 'sqlite',
        'source': DB_SOURCE,
        'connected': False,
        'init_error': None if DB_INIT_ERROR is None else 'database initialization failed (see server logs)',
    }
    try:
        from sqlalchemy import text as sa_text
        db.session.execute(sa_text('SELECT 1'))
        info['connected'] = True
        info['counts'] = {
            'users': User.query.count(),
            'telegram_accounts': TelegramAccount.query.count(),
            'scheduled_broadcasts': ScheduledBroadcast.query.count(),
            'auto_video_campaigns': AutoVideoPostCampaign.query.count(),
            'saved_targets': SavedTarget.query.count(),
            'task_runs': TaskRun.query.count(),
        }
    except Exception as e:
        info['connected'] = False
        info['error'] = friendly_telegram_error(e)
    # Only admins see account-wide counts.
    if not is_admin_user():
        info.pop('counts', None)
    return jsonify(info)


@app.route('/bot/saved_targets')
@login_required
def saved_targets_list():
    items = (SavedTarget.query.filter_by(user_id=current_user.id)
             .order_by(SavedTarget.created_at.desc()).all())
    return jsonify({'success': True, 'targets': [{
        'id': t.id, 'kind': t.kind, 'identifier': t.identifier,
        'title': t.title or '', 'notes': t.notes or '',
        'created_at': t.created_at.strftime('%Y-%m-%d %H:%M') if t.created_at else '',
    } for t in items]})


@app.route('/bot/saved_targets', methods=['POST'])
@login_required
def saved_targets_save():
    data = request.get_json() or {}
    kind = clean_str(data.get('kind'), 'channel') or 'channel'
    raw = data.get('identifiers') or data.get('identifier') or ''
    identifiers = clean_str_list(raw, split_lines_only=isinstance(raw, str) and '\n' in raw)
    if kind not in ('channel', 'group'):
        kind = 'channel'
    if not identifiers:
        return jsonify({'success': False, 'error': 'Nothing to save'}), 400
    added, invalid = 0, []
    for identifier in identifiers:
        parsed = parse_channel_target(identifier)
        if not parsed:
            invalid.append(identifier)
            continue
        value = parsed['value'] if parsed['kind'] in ('username', 'id') else identifier.strip()
        existing = SavedTarget.query.filter_by(user_id=current_user.id, identifier=value).first()
        if existing:
            continue
        db.session.add(SavedTarget(
            user_id=current_user.id, kind=kind, identifier=value,
            title=clean_str(data.get('title')) or None,
        ))
        added += 1
    db.session.commit()
    return jsonify({'success': True, 'added': added, 'invalid': invalid})


@app.route('/bot/saved_targets/<int:target_id>', methods=['DELETE'])
@login_required
def saved_targets_delete(target_id):
    item = SavedTarget.query.filter_by(id=target_id, user_id=current_user.id).first()
    if not item:
        return jsonify({'success': False, 'error': 'Not found'}), 404
    db.session.delete(item)
    db.session.commit()
    return jsonify({'success': True})

# ═══════════════════════════════════════════════════════════════════════════════
# ─── CHANNEL POST SCHEDULER ───────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/channel_post/create', methods=['POST'])
@login_required
def channel_post_create():
    data = request.get_json()
    account_id      = safe_int(data.get('account_id'), 0)
    channel         = data.get('channel', '').strip()
    content         = data.get('content', '').strip()
    message_type    = data.get('message_type', 'text')
    media_path      = clean_str(data.get('media_path'))
    dt_local        = data.get('scheduled_at', '')
    repeat_interval = safe_int(data.get('repeat_interval'), 0)
    if not channel or not (content or media_path) or not dt_local:
        return jsonify({'success': False, 'error': 'Fill all fields'})
    acc = get_account_for_current_user(account_id)
    if not acc:
        return jsonify({'success': False, 'error': 'Account not found'})
    try:
        scheduled_at = datetime.fromisoformat(dt_local.replace('Z',''))
    except Exception:
        return jsonify({'success': False, 'error': 'Invalid date/time'})
    post = ChannelScheduledPost(user_id=current_user.id, account_id=account_id,
                                channel=channel, content=content,
                                message_type=message_type, scheduled_at=scheduled_at,
                                repeat_interval=repeat_interval,
                                media_path=media_path or None)
    db.session.add(post)
    db.session.commit()
    return jsonify({'success': True, 'message': f'Post scheduled for {scheduled_at.strftime("%Y-%m-%d %H:%M")}'})


@app.route('/bot/channel_post/list')
@login_required
def channel_post_list():
    posts = ChannelScheduledPost.query.filter_by(user_id=current_user.id).order_by(ChannelScheduledPost.scheduled_at.desc()).limit(MAX_LIST_ROWS).all()
    return jsonify({'success': True, 'posts': [
        {'id': p.id, 'channel': p.channel, 'content': p.content[:80],
         'message_type': p.message_type, 'scheduled_at': p.scheduled_at.strftime('%Y-%m-%d %H:%M'),
         'status': p.status, 'repeat_interval': p.repeat_interval} for p in posts]})


@app.route('/bot/channel_post/delete/<int:post_id>', methods=['DELETE'])
@login_required
def channel_post_delete(post_id):
    post = ChannelScheduledPost.query.filter_by(id=post_id, user_id=current_user.id).first()
    if not post:
        return jsonify({'success': False, 'error': 'Not found'})
    db.session.delete(post)
    db.session.commit()
    return jsonify({'success': True})


def run_channel_post_scheduler():
    """Background thread: send pending channel posts."""
    def worker():
        while True:
            time.sleep(30)
            try:
                with app.app_context():
                    now = datetime.utcnow()
                    pending = ChannelScheduledPost.query.filter(
                        ChannelScheduledPost.status == 'pending',
                        ChannelScheduledPost.scheduled_at <= now
                    ).all()
                    for post in pending:
                        post.status = 'running'
                        db.session.commit()
                        acc = TelegramAccount.query.get(post.account_id)
                        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
                            post.status = 'failed'; db.session.commit(); continue
                        api_id, api_hash = get_api_credentials(post.user_id)
                        if not api_id:
                            post.status = 'failed'; db.session.commit(); continue
                        proxy   = get_proxy_for_account(post.user_id, post.account_id)
                        post_id = post.id
                        channel = post.channel
                        content = post.content
                        mtype   = post.message_type
                        media   = post.media_path
                        sess    = acc.session_string
                        repeat  = post.repeat_interval
                        user_id = post.user_id
                        account_id = post.account_id

                        async def _send(sess=sess, api_id=api_id, api_hash=api_hash, proxy=proxy,
                                        channel=channel, content=content, mtype=mtype, media=media):
                            client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                            await client.connect()
                            try:
                                entity = await client.get_entity(channel)
                                if media:
                                    # Device-uploaded photo/video: the text becomes the caption.
                                    if not os.path.exists(media):
                                        raise FileNotFoundError(
                                            'Uploaded media is no longer on the server '
                                            '(ephemeral disk). Re-upload it for this post.')
                                    await client.send_file(entity, media, caption=content or None)
                                elif mtype == 'text' or not content:
                                    await client.send_message(entity, content)
                                else:
                                    await client.send_file(entity, content)
                            finally:
                                await client.disconnect()

                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(_send())
                            p = ChannelScheduledPost.query.get(post_id)
                            if p:
                                p.status = 'sent'
                                p.sent_at = datetime.utcnow()
                                db.session.commit()
                                if repeat > 0:
                                    new_post = ChannelScheduledPost(
                                        user_id=p.user_id, account_id=p.account_id,
                                        channel=p.channel, content=p.content,
                                        message_type=p.message_type,
                                        scheduled_at=datetime.utcnow() + timedelta(hours=repeat),
                                        repeat_interval=repeat)
                                    db.session.add(new_post)
                                    db.session.commit()
                            log_activity(user_id, f"Channel post sent to {channel}", 'success', account_id)
                        except Exception as e:
                            p = ChannelScheduledPost.query.get(post_id)
                            if p:
                                p.status = 'failed'; db.session.commit()
                            logger.error(f"channel_post_scheduler {channel}: {e}")
                        finally:
                            loop.close()
            except Exception as e:
                logger.error(f"run_channel_post_scheduler: {e}")

    t = threading.Thread(target=worker, daemon=True)
    t.start()


# ═══════════════════════════════════════════════════════════════════════════════
# ─── SCHEDULED SCRAPE + DM ────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/scrape_dm/create', methods=['POST'])
@login_required
def scrape_dm_create():
    data = request.get_json()
    account_id   = safe_int(data.get('account_id'), 0)
    group_link   = data.get('group_link', '').strip()
    scrape_limit = safe_int(data.get('scrape_limit'), 100, minimum=1, maximum=MAX_SCRAPE_LIMIT)
    active_only  = bool(data.get('active_only', True))
    dm_message   = data.get('dm_message', '').strip()
    dm_delay     = max(3, safe_int(data.get('dm_delay'), 5))
    dt_local     = data.get('scheduled_at', '')
    if not group_link or not dm_message or not dt_local:
        return jsonify({'success': False, 'error': 'Fill all fields'})
    if is_protected_target(group_link):
        return jsonify({'success': False, 'error': 'Protected target'})
    acc = get_account_for_current_user(account_id)
    if not acc:
        return jsonify({'success': False, 'error': 'Account not found'})
    try:
        scheduled_at = datetime.fromisoformat(dt_local.replace('Z',''))
    except Exception:
        return jsonify({'success': False, 'error': 'Invalid date/time'})
    task = ScrapeDMTask(user_id=current_user.id, account_id=account_id,
                        group_link=group_link, scrape_limit=scrape_limit,
                        active_only=active_only, dm_message=dm_message,
                        dm_delay=dm_delay, scheduled_at=scheduled_at)
    db.session.add(task)
    db.session.commit()
    return jsonify({'success': True, 'message': f'Scrape+DM task scheduled for {scheduled_at.strftime("%Y-%m-%d %H:%M")}'})


@app.route('/bot/scrape_dm/list')
@login_required
def scrape_dm_list():
    tasks = ScrapeDMTask.query.filter_by(user_id=current_user.id).order_by(ScrapeDMTask.scheduled_at.desc()).limit(MAX_LIST_ROWS).all()
    return jsonify({'success': True, 'tasks': [
        {'id': t.id, 'group_link': t.group_link, 'scrape_limit': t.scrape_limit,
         'dm_message': t.dm_message[:60], 'scheduled_at': t.scheduled_at.strftime('%Y-%m-%d %H:%M'),
         'status': t.status, 'scraped': t.scraped, 'sent': t.sent} for t in tasks]})


@app.route('/bot/scrape_dm/delete/<int:task_id>', methods=['DELETE'])
@login_required
def scrape_dm_delete(task_id):
    task = ScrapeDMTask.query.filter_by(id=task_id, user_id=current_user.id).first()
    if not task:
        return jsonify({'success': False, 'error': 'Not found'})
    db.session.delete(task)
    db.session.commit()
    return jsonify({'success': True})


def run_scrape_dm_scheduler():
    """Background thread: execute pending Scrape+DM tasks."""
    def worker():
        while True:
            time.sleep(40)
            try:
                with app.app_context():
                    now = datetime.utcnow()
                    pending = ScrapeDMTask.query.filter(
                        ScrapeDMTask.status == 'pending',
                        ScrapeDMTask.scheduled_at <= now
                    ).all()
                    for task in pending:
                        task.status = 'running'
                        db.session.commit()
                        acc = TelegramAccount.query.get(task.account_id)
                        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
                            task.status = 'failed'; db.session.commit(); continue
                        api_id, api_hash = get_api_credentials(task.user_id)
                        if not api_id:
                            task.status = 'failed'; db.session.commit(); continue
                        proxy       = get_proxy_for_account(task.user_id, task.account_id)
                        task_id_    = task.id
                        group_link  = task.group_link
                        scrape_limit= task.scrape_limit
                        active_only = task.active_only
                        dm_message  = task.dm_message
                        dm_delay    = task.dm_delay
                        sess        = acc.session_string
                        user_id     = task.user_id
                        account_id  = task.account_id

                        def run_task(task_id=task_id_, sess=sess, api_id=api_id, api_hash=api_hash,
                                     proxy=proxy, group_link=group_link, scrape_limit=scrape_limit,
                                     active_only=active_only, dm_message=dm_message, dm_delay=dm_delay,
                                     user_id=user_id, account_id=account_id):
                            with app.app_context():
                                async def _run():
                                    client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                                    await client.connect()
                                    scraped = 0; sent = 0
                                    try:
                                        entity = await client.get_entity(group_link)
                                        cutoff = datetime.utcnow() - timedelta(hours=24)
                                        members = []
                                        async for user in client.iter_participants(entity, limit=scrape_limit):
                                            if user.bot or not user.username:
                                                continue
                                            status = getattr(user, 'status', None)
                                            is_online   = isinstance(status, types.UserStatusOnline)
                                            is_recently = isinstance(status, types.UserStatusRecently)
                                            is_24h = (isinstance(status, types.UserStatusOffline)
                                                      and getattr(status, 'was_online', None) is not None
                                                      and status.was_online.replace(tzinfo=None) >= cutoff)
                                            if active_only and not (is_online or is_recently or is_24h):
                                                continue
                                            members.append(user)
                                        scraped = len(members)
                                        for user in members:
                                            try:
                                                await client.send_message(user, dm_message)
                                                sent += 1
                                                await asyncio.sleep(dm_delay)
                                            except FloodWaitError as e:
                                                await asyncio.sleep(min(e.seconds, 60))
                                            except Exception:
                                                pass
                                    finally:
                                        await client.disconnect()
                                    return scraped, sent

                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                try:
                                    scraped, sent = loop.run_until_complete(_run())
                                    t = ScrapeDMTask.query.get(task_id)
                                    if t:
                                        t.status = 'completed'
                                        t.scraped = scraped; t.sent = sent
                                        db.session.commit()
                                    log_activity(user_id, f"Scrape+DM: scraped {scraped}, sent {sent} DMs from {group_link}", 'success', account_id)
                                except Exception as e:
                                    t = ScrapeDMTask.query.get(task_id)
                                    if t:
                                        t.status = 'failed'; db.session.commit()
                                    logger.error(f"scrape_dm_task: {e}")
                                finally:
                                    loop.close()

                        threading.Thread(target=run_task, daemon=True).start()
            except Exception as e:
                logger.error(f"run_scrape_dm_scheduler: {e}")

    t = threading.Thread(target=worker, daemon=True)
    t.start()


# ═══════════════════════════════════════════════════════════════════════════════
# ─── USERNAME CHECKER ─────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/check_usernames', methods=['POST'])
@login_required
def check_usernames():
    try:
        data = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        usernames  = [u.strip().lstrip('@') for u in data.get('usernames', []) if u.strip()]
        if not usernames:
            return jsonify({'success': False, 'error': 'Enter usernames to check'})
        acc = get_account_for_current_user(account_id)
        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})
        api_id, api_hash = get_api_credentials(acc.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not set'})
        proxy = get_proxy_for_account(acc.user_id, account_id)

        async def _check():
            client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            results = []
            try:
                for uname in usernames[:200]:
                    try:
                        entity = await client.get_entity(uname)
                        results.append({'username': uname, 'valid': True,
                                        'name': getattr(entity, 'first_name', '') or getattr(entity, 'title', ''),
                                        'id': entity.id})
                    except Exception as e:
                        results.append({'username': uname, 'valid': False, 'error': str(e)[:60]})
                    await asyncio.sleep(0.5)
            finally:
                await client.disconnect()
            return results

        results = run_async(_check())
        valid   = sum(1 for r in results if r['valid'])
        invalid = len(results) - valid
        return jsonify({'success': True, 'results': results, 'valid': valid, 'invalid': invalid})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ─── USER MANAGEMENT (ADMIN) ──────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/admin/list_users')
@login_required
def umgmt_list_users():
    if not is_admin_user():
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    users = User.query.order_by(User.created_at.desc()).all()
    result = []
    for u in users:
        acct_count = TelegramAccount.query.filter_by(user_id=u.id).count()
        result.append({'id': u.id, 'username': u.username,
                       'created_at': u.created_at.strftime('%Y-%m-%d'),
                       'accounts': acct_count,
                       'is_banned': getattr(u, 'is_banned', False)})
    return jsonify({'success': True, 'users': result})


@app.route('/bot/admin/create_user', methods=['POST'])
@login_required
def umgmt_create_user():
    if not is_admin_user():
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    data     = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()
    if not username or not password:
        return jsonify({'success': False, 'error': 'Username and password required'})
    if User.query.filter_by(username=username).first():
        return jsonify({'success': False, 'error': 'Username already exists'})
    user = User(username=username, password=generate_password_hash(password))
    db.session.add(user)
    db.session.commit()
    return jsonify({'success': True, 'message': f'User {username} created'})


@app.route('/bot/admin/change_password', methods=['POST'])
@login_required
def umgmt_change_password():
    if not is_admin_user():
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    data     = request.get_json()
    user_id  = safe_int(data.get('user_id'), 0)
    password = data.get('password', '').strip()
    if not password:
        return jsonify({'success': False, 'error': 'Password required'})
    user = User.query.get(user_id)
    if not user:
        return jsonify({'success': False, 'error': 'User not found'})
    user.password = generate_password_hash(password)
    db.session.commit()
    return jsonify({'success': True, 'message': f'Password changed for {user.username}'})


@app.route('/bot/admin/ban_user', methods=['POST'])
@login_required
def umgmt_ban_user():
    if not is_admin_user():
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    data    = request.get_json()
    user_id = safe_int(data.get('user_id'), 0)
    ban     = bool(data.get('ban', True))
    user = User.query.get(user_id)
    if not user:
        return jsonify({'success': False, 'error': 'User not found'})
    if user.username in ADMIN_USERS:
        return jsonify({'success': False, 'error': 'Cannot ban admin'})
    try:
        user.is_banned = ban
    except Exception:
        pass
    db.session.commit()
    action = 'banned' if ban else 'unbanned'
    return jsonify({'success': True, 'message': f'User {user.username} {action}'})


@app.route('/bot/admin/delete_user_post', methods=['POST'])
@login_required
def umgmt_delete_user():
    if not is_admin_user():
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    data    = request.get_json()
    user_id = safe_int(data.get('user_id'), 0)
    user = User.query.get(user_id)
    if not user:
        return jsonify({'success': False, 'error': 'User not found'})
    if user.username in ADMIN_USERS:
        return jsonify({'success': False, 'error': 'Cannot delete admin'})
    db.session.delete(user)
    db.session.commit()
    return jsonify({'success': True, 'message': f'User deleted'})


# ═══════════════════════════════════════════════════════════════════════════════
# ─── CHANNEL SCRAPER ──────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/scrape_channel', methods=['POST'])
@login_required
def scrape_channel():
    try:
        data       = request.get_json()
        account_id = safe_int(data.get('account_id'), 0)
        channel    = data.get('channel', '').strip()
        limit      = safe_int(data.get('limit'), 200, minimum=1, maximum=MAX_SCRAPE_LIMIT)
        if not channel:
            return jsonify({'success': False, 'error': 'Channel link required'})
        if is_protected_target(channel):
            return jsonify({'success': False, 'error': 'Protected target'})
        acc = get_account_for_current_user(account_id)
        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not connected'})
        api_id, api_hash = get_api_credentials(acc.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not set'})
        proxy = get_proxy_for_account(acc.user_id, account_id)

        async def _scrape():
            client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            members = []
            try:
                entity = await client.get_entity(channel)
                async for user in client.iter_participants(entity, limit=limit):
                    if user.bot or not user.username:
                        continue
                    members.append({'id': user.id, 'username': user.username,
                                    'first_name': user.first_name or '',
                                    'last_name': user.last_name or ''})
            finally:
                await client.disconnect()
            return members

        members = run_async(_scrape())
        acc.members_scraped += len(members)
        acc.last_used = datetime.utcnow()
        db.session.commit()
        log_activity(current_user.id, f"Channel scrape: {len(members)} members from {channel}", 'success', account_id)
        return jsonify({'success': True, 'members': members, 'count': len(members)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ─── VIEW BOOSTER (Post Views) ────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/boost_post_views', methods=['POST'])
@login_required
def boost_post_views():
    try:
        data        = request.get_json()
        account_ids = [safe_int(x, 0) for x in (data.get('account_ids') or []) if safe_int(x, 0)]
        channel     = data.get('channel', '').strip()
        post_limit  = safe_int(data.get('post_limit'), 10, minimum=1, maximum=MAX_BULK_COUNT)
        if not channel or not account_ids:
            return jsonify({'success': False, 'error': 'Channel and accounts required'})
        if is_protected_target(channel):
            return jsonify({'success': False, 'error': 'Protected target'})

        valid_accounts = []
        for aid in account_ids:
            a = get_account_for_current_user(aid)
            if a and a.session_string and a.session_string not in ('demo', 'active'):
                valid_accounts.append(a)
        if not valid_accounts:
            return jsonify({'success': False, 'error': 'No valid accounts'})

        api_id, api_hash = get_api_credentials(current_user.id)
        if not api_id:
            return jsonify({'success': False, 'error': 'API credentials not set'})

        user_id     = current_user.id
        total_views = [0]

        def run_view_boost(acc):
            with app.app_context():
                proxy = get_proxy_for_account(user_id, acc.id)
                async def _view():
                    from telethon.tl.functions.messages import GetMessagesViewsRequest
                    client = TelegramClient(StringSession(acc.session_string), api_id, api_hash, proxy=proxy)
                    await client.connect()
                    viewed = 0
                    try:
                        ch_name = parse_channel_username(channel)
                        try:
                            entity = await client.get_entity(ch_name)
                        except Exception:
                            await client(ImportChatInviteRequest(ch_name)) if ch_name.startswith('+') else None
                            entity = await client.get_entity(ch_name)
                        msgs    = await client.get_messages(entity, limit=post_limit)
                        msg_ids = [m.id for m in msgs]
                        if msg_ids:
                            await client(GetMessagesViewsRequest(peer=entity, id=msg_ids, increment=True))
                            viewed = len(msg_ids)
                    except Exception as e:
                        logger.warning(f"boost_post_views {acc.phone}: {e}")
                    finally:
                        await client.disconnect()
                    return viewed

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    viewed = loop.run_until_complete(_view())
                    total_views[0] += viewed
                    log_activity(user_id, f"Boosted {viewed} post views on {channel} via {acc.phone}", 'success', acc.id)
                except Exception as e:
                    logger.error(f"boost_post_views error {acc.phone}: {e}")
                finally:
                    loop.close()

        threads = [threading.Thread(target=run_view_boost, args=(acc,), daemon=True) for acc in valid_accounts]
        for t in threads: t.start()
        for t in threads: t.join(timeout=60)
        return jsonify({'success': True, 'message': f'Boosted views on {total_views[0]} posts using {len(valid_accounts)} accounts',
                        'total_views': total_views[0]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ─── NEW MEMBER ALERT BOT ─────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/bot/member_alert/save', methods=['POST'])
@login_required
def member_alert_save():
    data       = request.get_json()
    account_id = safe_int(data.get('account_id'), 0)
    group      = data.get('group', '').strip()
    welcome    = data.get('welcome_msg', '').strip()
    delay      = max(3, safe_int(data.get('delay'), 5))
    enabled    = bool(data.get('enabled', True))
    if not group or not welcome:
        return jsonify({'success': False, 'error': 'Group and welcome message required'})
    if is_protected_target(group):
        return jsonify({'success': False, 'error': 'Protected target'})
    acc = get_account_for_current_user(account_id)
    if not acc:
        return jsonify({'success': False, 'error': 'Account not found'})
    existing = MemberAlertBot.query.filter_by(user_id=current_user.id, group=group).first()
    if existing:
        existing.account_id = account_id
        existing.welcome_msg = welcome
        existing.delay = delay
        existing.enabled = enabled
    else:
        bot = MemberAlertBot(user_id=current_user.id, account_id=account_id,
                             group=group, welcome_msg=welcome, delay=delay, enabled=enabled)
        db.session.add(bot)
    db.session.commit()
    return jsonify({'success': True, 'message': 'Member alert bot saved'})


@app.route('/bot/member_alert/list')
@login_required
def member_alert_list():
    bots = MemberAlertBot.query.filter_by(user_id=current_user.id).all()
    return jsonify({'success': True, 'bots': [
        {'id': b.id, 'group': b.group, 'welcome_msg': b.welcome_msg[:60],
         'delay': b.delay, 'enabled': b.enabled,
         'account_id': b.account_id} for b in bots]})


@app.route('/bot/member_alert/delete/<int:bot_id>', methods=['DELETE'])
@login_required
def member_alert_delete(bot_id):
    bot = MemberAlertBot.query.filter_by(id=bot_id, user_id=current_user.id).first()
    if not bot:
        return jsonify({'success': False, 'error': 'Not found'})
    db.session.delete(bot)
    db.session.commit()
    return jsonify({'success': True})


def run_member_alert_bot():
    """Background thread: detect new group members and send them a welcome DM."""
    def worker():
        # track {bot_id: set of known member user_ids}
        known_members: dict = {}
        while True:
            time.sleep(60)
            try:
                with app.app_context():
                    bots = MemberAlertBot.query.filter_by(enabled=True).all()
                    for bot in bots:
                        acc = TelegramAccount.query.get(bot.account_id)
                        if not acc or not acc.session_string or acc.session_string in ('demo', 'active'):
                            continue
                        api_id, api_hash = get_api_credentials(bot.user_id)
                        if not api_id:
                            continue
                        proxy    = get_proxy_for_account(bot.user_id, bot.account_id)
                        bot_id   = bot.id
                        group    = bot.group
                        welcome  = bot.welcome_msg
                        delay    = bot.delay
                        sess     = acc.session_string
                        user_id  = bot.user_id
                        account_id = bot.account_id

                        async def _check(sess=sess, api_id=api_id, api_hash=api_hash,
                                         proxy=proxy, group=group, welcome=welcome,
                                         delay=delay, bot_id=bot_id):
                            client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
                            await client.connect()
                            new_users = []
                            try:
                                entity = await client.get_entity(group)
                                current_ids = set()
                                async for user in client.iter_participants(entity, limit=500):
                                    if not user.bot:
                                        current_ids.add(user.id)
                                prev_ids = known_members.get(bot_id)
                                if prev_ids is None:
                                    known_members[bot_id] = current_ids
                                else:
                                    new_ids = current_ids - prev_ids
                                    known_members[bot_id] = current_ids
                                    async for user in client.iter_participants(entity, limit=500):
                                        if user.id in new_ids and not user.bot:
                                            try:
                                                await client.send_message(user, welcome)
                                                new_users.append(user.id)
                                                await asyncio.sleep(1)  # 1s safety gap between targets — a whole pass now finishes fast
                                            except Exception:
                                                pass
                            finally:
                                await client.disconnect()
                            return new_users

                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            new_users = loop.run_until_complete(_check())
                            if new_users:
                                log_activity(user_id, f"Member alert: welcomed {len(new_users)} new members in {group}", 'success', account_id)
                        except Exception as e:
                            logger.warning(f"member_alert_bot {group}: {e}")
                        finally:
                            loop.close()
            except Exception as e:
                logger.error(f"run_member_alert_bot: {e}")

    t = threading.Thread(target=worker, daemon=True)
    t.start()

# ═══════════════════════════════════════════════════════════════════════════════
# ─── GENERIC SCHEDULER (one scheduling engine for every messaging tool) ───────
# ═══════════════════════════════════════════════════════════════════════════════
# Group Broadcast, Scheduled Broadcasts, Personal Chats, Bulk DMs, DM Campaigns,
# Group-Link messaging and Channel Posts all hand their settings to this engine:
#   • send now or schedule for later (one-time or recurring)
#   • edit / pause / resume / cancel / delete pending tasks
#   • pending / running / completed / partial / failed / paused / cancelled
#   • durable in PostgreSQL -> survives redeploys and restarts
#   • a due task is claimed exactly once (DB state guard + TaskRun), so a restart
#     can never run the same schedule twice
#   • per-destination result with explicit reason; Telegram rate limits are waited
#     out and the destination is reported as skipped (never bypassed)

SCHEDULER_KINDS = {
    'broadcast':    ('Group Broadcast', True, True),
    'group_links':  ('Group-Link Message', True, True),
    'dm':           ('Bulk Direct Messages', True, False),
    'dm_campaign':  ('DM Campaign', True, False),
    'personal':     ('Personal Chat Message', True, False),
    'channel_post': ('Channel Post', True, True),
}


class ScheduledTask(db.Model):
    """A scheduled (or immediately-run) job created by any messaging tool."""
    id                      = db.Column(db.Integer, primary_key=True)
    user_id                 = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    kind                    = db.Column(db.String(40), nullable=False)
    title                   = db.Column(db.String(200), nullable=True)
    account_ids             = db.Column(db.Text, nullable=False)
    destinations            = db.Column(db.Text, nullable=False)
    message                 = db.Column(db.Text, nullable=True)
    caption                 = db.Column(db.Text, nullable=True)
    media_path              = db.Column(db.Text, nullable=True)
    message_type            = db.Column(db.String(20), default='text')
    delay_seconds           = db.Column(db.Integer, default=5)
    scheduled_at            = db.Column(db.DateTime, nullable=False)
    repeat_interval_minutes = db.Column(db.Integer, default=0)
    repeat_count            = db.Column(db.Integer, default=0)
    runs_done               = db.Column(db.Integer, default=0)
    status                  = db.Column(db.String(20), default='pending')
    sent                    = db.Column(db.Integer, default=0)
    failed                  = db.Column(db.Integer, default=0)
    skipped                 = db.Column(db.Integer, default=0)
    total                   = db.Column(db.Integer, default=0)
    errors                  = db.Column(db.Text, nullable=True)
    last_run_at             = db.Column(db.DateTime, nullable=True)
    next_run_at             = db.Column(db.DateTime, nullable=True)
    claimed_by              = db.Column(db.String(80), nullable=True)
    claimed_at              = db.Column(db.DateTime, nullable=True)
    created_at              = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at              = db.Column(db.DateTime, default=datetime.utcnow)

    def destinations_list(self):
        try:
            return [clean_str(d) for d in json.loads(self.destinations or '[]') if clean_str(d)]
        except Exception:
            return []

    def account_id_list(self):
        try:
            return [safe_int(a, 0) for a in json.loads(self.account_ids or '[]') if safe_int(a, 0)]
        except Exception:
            return []

    def to_dict(self):
        dests = self.destinations_list()
        meta = SCHEDULER_KINDS.get(self.kind, (self.kind, False, False))
        return {
            'id': self.id,
            'kind': self.kind,
            'kind_label': meta[0],
            'kind_supports_media': meta[2],
            'title': self.title or '',
            'account_ids': self.account_id_list(),
            'destinations': dests,
            'destination_count': len(dests),
            'message': (self.message or '')[:300],
            'message_type': self.message_type or 'text',
            'has_media': bool(self.media_path),
            'delay_seconds': self.delay_seconds or 5,
            'scheduled_at': self.scheduled_at.strftime('%Y-%m-%d %H:%M') if self.scheduled_at else None,
            'scheduled_at_iso': self.scheduled_at.isoformat() if self.scheduled_at else None,
            'repeat_interval_minutes': self.repeat_interval_minutes or 0,
            'repeat_count': self.repeat_count or 0,
            'runs_done': self.runs_done or 0,
            'status': self.status,
            'sent': self.sent or 0,
            'failed': self.failed or 0,
            'skipped': self.skipped or 0,
            'total': self.total or 0,
            'errors': (self.errors or '').splitlines()[-8:],
            'last_run_at': self.last_run_at.strftime('%Y-%m-%d %H:%M') if self.last_run_at else None,
            'next_run_at': self.next_run_at.strftime('%Y-%m-%d %H:%M') if self.next_run_at else None,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M') if self.created_at else None,
            'can_edit': self.status in ('pending', 'paused'),
            'can_pause': self.status == 'pending',
            'can_resume': self.status == 'paused',
            'can_cancel': self.status in ('pending', 'paused', 'running'),
            'can_run_now': self.status in ('pending', 'paused'),
        }


# ─── Destination (group-link) helpers ────────────────────────────────────────

def normalize_destination_list(raw):
    """Normalise destinations (one per line) and report unusable entries."""
    entries = clean_str_list(raw, split_lines_only=True)
    valid, invalid, seen = [], [], set()
    for entry in entries:
        parsed = parse_channel_target(entry)
        if not parsed:
            invalid.append(entry)
            continue
        value = entry.strip()          # keep the operator's original text for display
        # De-duplicate on the NORMALISED key so @alpha, t.me/alpha and
        # https://t.me/alpha are recognised as the same destination.
        key = '%s:%s' % (parsed['kind'], parsed['value'].lstrip('@').lower())
        if key in seen:
            continue
        seen.add(key)
        valid.append(value)
    return valid, invalid


async def resolve_destinations(client, destinations):
    """Resolve each destination and report availability (never raises per item)."""
    results = []
    for raw in destinations:
        info = parse_channel_target(raw)
        if not info:
            results.append({'destination': raw, 'ok': False, 'reason': 'invalid link or username format'})
            continue
        try:
            entity = await _resolve_channel_entity(client, raw)
            title = (getattr(entity, 'title', None) or getattr(entity, 'username', None)
                     or str(getattr(entity, 'id', raw)))
            results.append({
                'destination': raw,
                'ok': True,
                'resolved_title': str(title),
                'resolved_id': getattr(entity, 'id', None),
                'kind': info['kind'],
            })
        except FloodWaitError as e:
            results.append({'destination': raw, 'ok': False,
                            'reason': 'rate limit — retry after %ss' % safe_int(getattr(e, 'seconds', 0), 5)})
        except Exception as e:
            results.append({'destination': raw, 'ok': False, 'reason': friendly_telegram_error(e)})
    return results


@app.route('/bot/group_links/resolve', methods=['POST'])
@login_required
def group_links_resolve():
    """Validate + resolve group/channel links with the selected account."""
    try:
        data = request.get_json() or {}
        account_id = safe_int(data.get('account_id'), 0)
        destinations, invalid = normalize_destination_list(
            data.get('links') or data.get('destinations') or '')
        if not account_id:
            return jsonify({'success': False, 'error': 'Select an account first'}), 400
        if not destinations and not invalid:
            return jsonify({'success': False, 'error': 'Add at least one group or channel link'}), 400

        account = get_account_for_current_user(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found or unauthorized'}), 403
        if not account.session_string or account.session_string in ('demo', 'active'):
            return jsonify({'success': False, 'error': 'Account not properly connected'}), 400
        api_id, api_hash = get_api_credentials(account.user_id)
        if not api_id:
            return jsonify({'success': False, 'error': 'Telegram API credentials not configured'}), 400

        proxy = get_proxy_for_account(account.user_id, account_id)

        async def work():
            client = TelegramClient(StringSession(account.session_string), api_id, api_hash, proxy=proxy)
            await client.connect()
            try:
                return await resolve_destinations(client, destinations)
            finally:
                await client.disconnect()

        resolved = run_async(work())
        ok_count = len([r for r in resolved if r['ok']])
        return jsonify({
            'success': True,
            'results': resolved,
            'resolved': ok_count,
            'unresolved': len(resolved) - ok_count,
            'invalid': invalid,
            'note': 'Links are resolved with the selected account. A destination is usable only when that '
                    'account can post there — membership, admin rights and Telegram limits still apply.',
        })
    except Exception as e:
        logger.error('group_links_resolve error: %s', e)
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


# ─── Generic scheduler API ───────────────────────────────────────────────────

@app.route('/bot/scheduler/kinds')
@login_required
def scheduler_kinds():
    return jsonify({'success': True, 'kinds': [
        {'kind': k, 'label': v[0], 'needs_destinations': v[1], 'supports_media': v[2]}
        for k, v in SCHEDULER_KINDS.items()
    ]})


@app.route('/bot/scheduler/create', methods=['POST'])
@login_required
def scheduler_create():
    """Create a scheduled or immediate job for any messaging tool."""
    try:
        data = request.get_json() or {}
        kind = clean_str(data.get('kind'), 'broadcast').lower()
        if kind not in SCHEDULER_KINDS:
            return jsonify({'success': False, 'error': 'Unknown task type: %s' % kind}), 400

        raw_accounts = data.get('account_ids')
        if raw_accounts is None and data.get('account_id') is not None:
            raw_accounts = [data.get('account_id')]
        account_ids = dedupe_preserve([safe_int(a, 0) for a in (raw_accounts or []) if safe_int(a, 0)])
        if not account_ids:
            return jsonify({'success': False, 'error': 'Select at least one account'}), 400

        valid_accounts, rejected = [], []
        for acc_id in account_ids:
            account = get_account_for_current_user(acc_id)
            if not account:
                rejected.append('%s: not found or not yours' % acc_id)
                continue
            if not account.session_string or account.session_string in ('demo', 'active'):
                rejected.append('%s: not connected' % account.phone)
                continue
            if not get_api_credentials(account.user_id)[0]:
                rejected.append('%s: API credentials missing' % account.phone)
                continue
            valid_accounts.append(account)
        if not valid_accounts:
            return jsonify({'success': False,
                            'error': 'No usable account. ' + '; '.join(rejected[:3])}), 400

        destinations, invalid_destinations = normalize_destination_list(
            data.get('destinations') or data.get('groups') or data.get('links') or '')
        if SCHEDULER_KINDS[kind][1] and not destinations:
            extra = (' — invalid: ' + ', '.join(invalid_destinations[:3])) if invalid_destinations else ''
            return jsonify({'success': False,
                            'error': 'Add at least one destination (group, channel, username or link)' + extra}), 400

        message = clean_str(data.get('message'))
        message_type = clean_str(data.get('message_type'), 'text') or 'text'
        media_path = clean_str(data.get('media_path'))
        if not message and not media_path:
            return jsonify({'success': False, 'error': 'Enter a message or provide media'}), 400

        when_text = clean_str(data.get('scheduled_at'))
        send_now = safe_bool(data.get('send_now'), False) or not when_text
        if send_now:
            scheduled_at = datetime.utcnow() + timedelta(seconds=5)
        else:
            scheduled_at = parse_datetime_utc(when_text)
            if scheduled_at is None:
                return jsonify({'success': False,
                                'error': 'Invalid date/time. Use YYYY-MM-DDTHH:MM (UTC)'}), 400
            if scheduled_at <= datetime.utcnow():
                return jsonify({'success': False, 'error': 'Scheduled time must be in the future'}), 400

        repeat_interval = safe_int(data.get('repeat_interval_minutes'), 0, minimum=0,
                                   maximum=MAX_REPEAT_INTERVAL_HOURS * 60)
        repeat_count = safe_int(data.get('repeat_count'), 0, minimum=0, maximum=MAX_REPEAT_COUNT)
        if repeat_count and not repeat_interval:
            return jsonify({'success': False, 'error': 'A repeat count needs a repeat interval'}), 400

        task = ScheduledTask(
            user_id=current_user.id,
            kind=kind,
            title=clean_str(data.get('title')) or SCHEDULER_KINDS[kind][0],
            account_ids=json.dumps([a.id for a in valid_accounts]),
            destinations=json.dumps(destinations),
            message=message,
            caption=clean_str(data.get('caption')) or message,
            media_path=media_path or None,
            message_type=message_type,
            delay_seconds=safe_int(data.get('delay'), 5, minimum=1, maximum=MAX_DELAY_SECONDS),
            scheduled_at=scheduled_at,
            repeat_interval_minutes=repeat_interval,
            repeat_count=repeat_count,
            runs_done=0,
            status='pending',
            total=len(destinations) * len(valid_accounts),
            next_run_at=scheduled_at,
        )
        db.session.add(task)
        db.session.commit()
        log_activity(
            current_user.id,
            'Scheduled %s for %d destination(s) with %d account(s)%s' % (
                SCHEDULER_KINDS[kind][0], len(destinations), len(valid_accounts),
                '' if send_now else ' at ' + scheduled_at.strftime('%Y-%m-%d %H:%M') + ' UTC'),
            'success', valid_accounts[0].id)
        payload = {'success': True, 'task': task.to_dict(), 'rejected': rejected,
                   'execution_strategy': 'destinations are spread round-robin across the selected accounts'}
        if invalid_destinations:
            payload['invalid_destinations'] = invalid_destinations[:10]
        return jsonify(payload)
    except Exception as e:
        logger.exception('scheduler_create failed')
        return jsonify({'success': False, 'error': friendly_telegram_error(e)}), 500


@app.route('/bot/scheduler/list')
@login_required
def scheduler_list():
    status_filter = clean_str(request.args.get('status'))
    query = ScheduledTask.query.filter_by(user_id=current_user.id)
    if status_filter and status_filter != 'all':
        query = query.filter(ScheduledTask.status == status_filter)
    tasks = query.order_by(ScheduledTask.scheduled_at.asc()).limit(MAX_LIST_ROWS).all()
    counts = {}
    for row in ScheduledTask.query.filter_by(user_id=current_user.id).all():
        counts[row.status] = counts.get(row.status, 0) + 1
    return jsonify({'success': True, 'tasks': [t.to_dict() for t in tasks],
                    'counts': counts, 'total': sum(counts.values())})


def _scheduler_task_for_user(task_id):
    return ScheduledTask.query.filter_by(id=task_id, user_id=current_user.id).first()


@app.route('/bot/scheduler/<int:task_id>/pause', methods=['POST'])
@login_required
def scheduler_pause(task_id):
    task = _scheduler_task_for_user(task_id)
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    if task.status != 'pending':
        return jsonify({'success': False,
                        'error': 'Only a pending task can be paused (current status: %s)' % task.status}), 400
    task.status = 'paused'
    task.updated_at = datetime.utcnow()
    db.session.commit()
    log_activity(current_user.id, 'Paused schedule #%d' % task.id, 'success')
    return jsonify({'success': True, 'task': task.to_dict()})


@app.route('/bot/scheduler/<int:task_id>/resume', methods=['POST'])
@login_required
def scheduler_resume(task_id):
    task = _scheduler_task_for_user(task_id)
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    if task.status != 'paused':
        return jsonify({'success': False,
                        'error': 'Only a paused task can be resumed (current status: %s)' % task.status}), 400
    task.status = 'pending'
    if not task.next_run_at or task.next_run_at < datetime.utcnow():
        task.next_run_at = datetime.utcnow() + timedelta(minutes=1)
    task.scheduled_at = task.next_run_at
    task.updated_at = datetime.utcnow()
    db.session.commit()
    log_activity(current_user.id, 'Resumed schedule #%d' % task.id, 'success')
    return jsonify({'success': True, 'task': task.to_dict()})


@app.route('/bot/scheduler/<int:task_id>/cancel', methods=['POST'])
@login_required
def scheduler_cancel(task_id):
    task = _scheduler_task_for_user(task_id)
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    if task.status in ('completed', 'cancelled'):
        return jsonify({'success': False,
                        'error': 'This task has already finished (%s)' % task.status}), 400
    task.status = 'cancelled'
    task.repeat_interval_minutes = 0
    task.repeat_count = 0
    task.updated_at = datetime.utcnow()
    db.session.commit()
    log_activity(current_user.id, 'Cancelled schedule #%d' % task.id, 'success')
    return jsonify({'success': True, 'task': task.to_dict()})


@app.route('/bot/scheduler/<int:task_id>/run_now', methods=['POST'])
@login_required
def scheduler_run_now(task_id):
    task = _scheduler_task_for_user(task_id)
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    if task.status == 'running':
        return jsonify({'success': False, 'error': 'This task is already running'}), 409
    if task.status == 'cancelled':
        return jsonify({'success': False, 'error': 'This task was cancelled — create a new one'}), 400
    task.status = 'pending'
    task.next_run_at = datetime.utcnow()
    task.scheduled_at = task.next_run_at
    task.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({'success': True, 'message': 'Task queued to run within a few seconds',
                    'task': task.to_dict()})


@app.route('/bot/scheduler/<int:task_id>', methods=['PATCH'])
@login_required
def scheduler_edit(task_id):
    """Edit a pending/paused task without losing its settings."""
    task = _scheduler_task_for_user(task_id)
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    if task.status not in ('pending', 'paused'):
        return jsonify({'success': False,
                        'error': 'Only pending or paused tasks can be edited (current: %s)' % task.status}), 400
    data = request.get_json() or {}
    if 'message' in data:
        task.message = clean_str(data.get('message'))
    if 'caption' in data:
        task.caption = clean_str(data.get('caption'))
    if 'title' in data:
        task.title = clean_str(data.get('title'))
    if 'media_path' in data:
        task.media_path = clean_str(data.get('media_path')) or None
    if 'message_type' in data:
        task.message_type = clean_str(data.get('message_type'), 'text') or 'text'
    if 'delay' in data:
        task.delay_seconds = safe_int(data.get('delay'), task.delay_seconds or 5,
                                      minimum=1, maximum=MAX_DELAY_SECONDS)
    if 'destinations' in data:
        destinations, invalid = normalize_destination_list(data.get('destinations'))
        if not destinations:
            return jsonify({'success': False, 'error': 'No valid destination provided',
                            'invalid': invalid[:5]}), 400
        task.destinations = json.dumps(destinations)
    if 'scheduled_at' in data:
        when = parse_datetime_utc(data.get('scheduled_at'))
        if when is None or when <= datetime.utcnow():
            return jsonify({'success': False,
                            'error': 'Scheduled time must be a future date/time (YYYY-MM-DDTHH:MM)'}), 400
        task.scheduled_at = when
        task.next_run_at = when
    if 'repeat_interval_minutes' in data:
        task.repeat_interval_minutes = safe_int(data.get('repeat_interval_minutes'), 0,
                                                minimum=0, maximum=MAX_REPEAT_INTERVAL_HOURS * 60)
    if 'repeat_count' in data:
        task.repeat_count = safe_int(data.get('repeat_count'), 0, minimum=0, maximum=MAX_REPEAT_COUNT)
    if task.repeat_count and not task.repeat_interval_minutes:
        return jsonify({'success': False, 'error': 'A repeat count needs a repeat interval'}), 400
    if not task.message and not task.media_path:
        return jsonify({'success': False, 'error': 'Enter a message or provide media'}), 400
    task.total = len(task.destinations_list()) * len(task.account_id_list())
    task.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({'success': True, 'task': task.to_dict()})


@app.route('/bot/scheduler/<int:task_id>', methods=['DELETE'])
@login_required
def scheduler_delete(task_id):
    task = _scheduler_task_for_user(task_id)
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    if task.status == 'running':
        return jsonify({'success': False,
                        'error': 'This task is running — cancel it first, then delete'}), 409
    db.session.delete(task)
    db.session.commit()
    log_activity(current_user.id, 'Deleted schedule #%d' % task_id, 'success')
    return jsonify({'success': True})


# ─── Scheduler worker ────────────────────────────────────────────────────────

def _scheduler_send_destination(account, task, destination):
    """Send one task to one destination -> ('sent'|'failed'|'skipped', note).

    The client is created INSIDE the coroutine so Telethon binds to the event
    loop that `run_async` actually runs: creating it in the worker thread (which
    has no event loop) made every send fail with
    "There is no current event loop in thread 'generic-scheduler'".
    """
    api_id, api_hash = get_api_credentials(account.user_id)
    proxy = get_proxy_for_account(account.user_id, account.id)
    sess = account.session_string
    kind = task.kind
    message = task.message
    caption = task.caption or task.message
    media_path = task.media_path
    message_type = task.message_type

    async def _send():
        client = TelegramClient(StringSession(sess), api_id, api_hash, proxy=proxy)
        await client.connect()
        try:
            if kind in ('dm', 'dm_campaign', 'personal'):
                parsed = parse_channel_target(destination)
                target = safe_int(parsed['value'], 0) if (parsed and parsed['kind'] == 'id') \
                    else clean_str(destination).lstrip('@')
                await client.send_message(target, message)
            else:
                entity = await _resolve_channel_entity(client, destination)
                if media_path and message_type in ('photo', 'video'):
                    await client.send_file(entity, media_path, caption=caption or None)
                else:
                    await client.send_message(entity, message)
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    attempt = 0
    while attempt < 2:
        attempt += 1
        try:
            run_async(_send())
            return 'sent', None
        except FloodWaitError as e:
            wait = min(safe_int(getattr(e, 'seconds', 0), 5), MAX_FLOOD_WAIT_SECONDS)
            time.sleep(wait)
            if attempt >= 2:
                return 'skipped', 'rate limit — skipped after waiting %ss' % wait
        except Exception as e:
            return 'failed', friendly_telegram_error(e, str(destination))
    return 'failed', 'unknown error'


def _scheduler_run_task(task_id):
    """Execute one due task exactly once and persist every counter change."""
    worker_id = 'worker:%d' % os.getpid()
    with app.app_context():
        # Claim guard: only one worker can move pending -> running.
        changed = ScheduledTask.query.filter(
            ScheduledTask.id == task_id,
            ScheduledTask.status == 'pending',
        ).update({
            ScheduledTask.status: 'running',
            ScheduledTask.claimed_by: worker_id,
            ScheduledTask.claimed_at: datetime.utcnow(),
        }, synchronize_session=False)
        db.session.commit()
        if not changed:
            logger.info('Scheduler: task %s already claimed or no longer pending', task_id)
            return False

        task = ScheduledTask.query.get(task_id)
        if not task:
            return False
        run_key = 'schedtask:%d:run%d' % (task.id, (task.runs_done or 0) + 1)
        task_run_start(task.user_id, 'schedule:' + task.kind,
                       len(task.destinations_list()) * len(task.account_id_list()), task_key=run_key)
        errors, sent, failed, skipped = [], 0, 0, 0
        try:
            destinations = task.destinations_list()
            accounts = [a for a in (TelegramAccount.query.get(i) for i in task.account_id_list()) if a]
            if not destinations:
                raise RuntimeError('task has no destinations')
            if not accounts:
                raise RuntimeError('task has no usable account')

            # Explicit strategy: destinations are spread round-robin over the
            # selected accounts. No silent account switching.
            for index, destination in enumerate(destinations):
                account = accounts[index % len(accounts)]
                api_id, api_hash = get_api_credentials(account.user_id)
                if not api_id:
                    failed += 1
                    errors.append('%s: API credentials missing' % destination)
                    continue
                status, note = _scheduler_send_destination(account, task, destination)
                if status == 'sent':
                    sent += 1
                elif status == 'skipped':
                    skipped += 1
                    errors.append('%s: %s' % (destination, note))
                else:
                    failed += 1
                    errors.append('%s: %s' % (destination, note or 'failed'))
                task.sent, task.failed, task.skipped = sent, failed, skipped
                task.errors = '\n'.join(errors[-20:]) if errors else None
                task.updated_at = datetime.utcnow()
                db.session.commit()
                task_run_update(run_key, sent=sent, failed=failed, skipped=skipped, errors=errors[-1:])
                if task.delay_seconds:
                    time.sleep(min(safe_int(task.delay_seconds, 5), 3600))

            task = ScheduledTask.query.get(task_id)
            if not task:
                return True
            task.runs_done = (task.runs_done or 0) + 1
            task.last_run_at = datetime.utcnow()
            task.sent, task.failed, task.skipped = sent, failed, skipped
            task.total = len(destinations) * len(accounts)
            # A run with zero confirmed sends is never reported as success.
            run_status = 'completed' if sent and not failed else ('failed' if not sent else 'partial')
            should_repeat, remaining = next_repeat_settings(task.repeat_interval_minutes, task.repeat_count)
            if should_repeat and task.repeat_interval_minutes > 0:
                task.repeat_count = remaining
                task.status = 'pending'
                task.next_run_at = datetime.utcnow() + timedelta(minutes=task.repeat_interval_minutes)
                task.scheduled_at = task.next_run_at
                task.claimed_by = None
                task.claimed_at = None
            else:
                task.status = run_status
                task.next_run_at = None
            task.errors = '\n'.join(errors[-20:]) if errors else None
            task.updated_at = datetime.utcnow()
            db.session.commit()
            task_run_finish(run_key, run_status, sent=sent, failed=failed, skipped=skipped)
            log_activity(
                task.user_id,
                '%s run: %d sent, %d failed, %d skipped (%s)' % (
                    SCHEDULER_KINDS.get(task.kind, (task.kind,))[0], sent, failed, skipped, run_status),
                'success' if sent else 'failed',
                accounts[0].id if accounts else None)
            return True
        except Exception as e:
            logger.exception('scheduler task %s failed', task_id)
            task = ScheduledTask.query.get(task_id)
            if task:
                task.status = 'failed'
                task.errors = '\n'.join((errors + [friendly_telegram_error(e)])[-20:])
                task.last_run_at = datetime.utcnow()
                task.claimed_by = None
                db.session.commit()
            task_run_finish(run_key, 'failed', errors=[friendly_telegram_error(e)])
            return False


def run_generic_scheduler():
    """Background loop: claim due tasks and run each one exactly once."""
    def worker():
        while True:
            time.sleep(15)
            try:
                with app.app_context():
                    now = datetime.utcnow()
                    stale = now - timedelta(minutes=STALE_TASK_MINUTES)
                    # A worker that died mid-run: re-arm only if nothing was sent.
                    for stuck in ScheduledTask.query.filter(ScheduledTask.status == 'running').all():
                        if stuck.claimed_at and stuck.claimed_at < stale:
                            stuck.status = 'pending' if not (stuck.sent or 0) else 'interrupted'
                            stuck.claimed_by = None
                            stuck.claimed_at = None
                            stuck.next_run_at = now + timedelta(minutes=1)
                    db.session.commit()

                    due = ScheduledTask.query.filter(
                        ScheduledTask.status == 'pending',
                        ScheduledTask.next_run_at.isnot(None),
                        ScheduledTask.next_run_at <= now,
                    ).order_by(ScheduledTask.next_run_at.asc()).limit(MAX_SCHEDULED_BATCH).all()
                    due_ids = [t.id for t in due]
                for task_id in due_ids:
                    _scheduler_run_task(task_id)
            except Exception:
                logger.exception('generic scheduler loop failed')
    threading.Thread(target=worker, daemon=True, name='generic-scheduler').start()




# ─── STARTUP / SCHEMA / MIGRATION ────────────────────────────────────────────

# Columns that may be missing on databases created by an older version.
# Format: (table, column, sqlite_type, postgres_type)
SCHEMA_COLUMNS = [
    ("channel_scheduled_post", "media_path", "TEXT", "TEXT"),
    ("account_health", "tg_name", "VARCHAR(200)", "VARCHAR(200)"),
    ("account_health", "tg_username", "VARCHAR(100)", "VARCHAR(100)"),
    ("account_health", "spambot_released", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT FALSE"),
    ("account_health", "spambot_result", "VARCHAR(500)", "VARCHAR(500)"),
    ("alert_settings", "tg_enabled", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT FALSE"),
    ("alert_settings", "email_enabled", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT FALSE"),
    ("scheduled_broadcast", "repeat_interval", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("scheduled_broadcast", "repeat_count", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("broadcast_task", "caption", "TEXT", "TEXT"),
    ("broadcast_task", "repeat", "INTEGER DEFAULT 1", "INTEGER DEFAULT 1"),
    ("broadcast_task", "rounds_done", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("broadcast_task", "next_round_at", "TIMESTAMP", "TIMESTAMP"),
    ("broadcast_task", "task_key", "VARCHAR(64)", "VARCHAR(64)"),
    ("user", "is_banned", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT FALSE"),
    ("user", "last_login_at", "TIMESTAMP", "TIMESTAMP"),
    ("auto_reply_bot", "provider", "VARCHAR(20) DEFAULT 'openai'", "VARCHAR(20) DEFAULT 'openai'"),
    ("auto_reply_bot", "gemini_key", "VARCHAR(300)", "VARCHAR(300)"),
    ("auto_reply_bot", "service_keywords", "TEXT", "TEXT"),
    ("auto_reply_bot", "account_context", "TEXT", "TEXT"),
    ("auto_video_post_campaign", "source_type", "VARCHAR(20) DEFAULT 'zip'", "VARCHAR(20) DEFAULT 'zip'"),
    ("auto_video_post_campaign", "source_ids", "TEXT", "TEXT"),
    ("auto_video_post_campaign", "queue_state", "TEXT", "TEXT"),
    ("auto_video_post_campaign", "posted_count", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("auto_video_post_campaign", "failed_count", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("auto_video_post_campaign", "skipped_count", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("auto_video_post_campaign", "rounds_completed", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0"),
    ("auto_video_post_campaign", "stop_requested", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT FALSE"),
    ("auto_video_post_campaign", "needs_reupload", "BOOLEAN DEFAULT 0", "BOOLEAN DEFAULT FALSE"),
    ("auto_video_post_campaign", "last_round_summary", "TEXT", "TEXT"),
    ("auto_video_post_campaign", "worker_heartbeat", "TIMESTAMP", "TIMESTAMP"),
    ("auto_video_post_campaign", "vault_state", "TEXT", "TEXT"),
    ("auto_video_post_campaign", "captions", "TEXT", "TEXT"),
]

# Set by init_app_schema() so API routes can report a clear database error.
DB_INIT_ERROR = None


def _migration_ddl(table, column, col_type, is_postgres):
    """Build an ADD COLUMN statement that is safe on the target database.

    PostgreSQL reserves the word `user`, and before this fix the unquoted
    `ALTER TABLE user ADD COLUMN is_banned ...` failed with a syntax error, so
    those columns were never created and every login query failed. Identifiers
    are quoted on PostgreSQL; SQLite keeps the plain form (it rejects double
    quotes in this position).
    """
    if is_postgres:
        safe_table = str(table).replace('"', '""')
        safe_column = str(column).replace('"', '""')
        return f'ALTER TABLE "{safe_table}" ADD COLUMN "{safe_column}" {col_type}'
    return f'ALTER TABLE {table} ADD COLUMN {column} {col_type}'


def run_schema_migration():
    """Add missing columns on the ACTIVE database (PostgreSQL/Neon or SQLite).

    The previous implementation opened the local SQLite file directly, so on
    PostgreSQL it silently did nothing. This version uses the SQLAlchemy
    inspector, is safe to run on every start, and never drops or rewrites data.
    """
    from sqlalchemy import inspect as sa_inspect, text as sa_text
    added = []
    try:
        inspector = sa_inspect(db.engine)
        existing_tables = set(inspector.get_table_names())
        is_pg = db.engine.dialect.name.startswith('postgres')
        for table, column, sqlite_type, pg_type in SCHEMA_COLUMNS:
            if table not in existing_tables:
                continue
            try:
                existing_columns = {c['name'] for c in inspector.get_columns(table)}
            except Exception:
                continue
            if column in existing_columns:
                continue
            col_type = pg_type if is_pg else sqlite_type
            try:
                with db.engine.begin() as conn:
                    conn.execute(sa_text(_migration_ddl(table, column, col_type, is_pg)))
                added.append(f'{table}.{column}')
            except Exception as e:
                # This is serious: a missing column makes ORM queries for that
                # table fail (that is exactly how login broke).
                logger.error('Schema migration could NOT add %s.%s: %s', table, column, e)
    except Exception as e:
        logger.error('Schema migration could not inspect the database: %s', e)
    if added:
        logger.info('Schema migration added %d column(s): %s', len(added), ', '.join(added))
    return added


def import_legacy_sqlite():
    """Import data from a legacy local SQLite file into the configured database.

    Disabled by default. Enable with MIGRATE_SQLITE=1 (always) or
    MIGRATE_SQLITE=auto (only when the target database has no users yet).
    Existing rows are never overwritten: users match on username, accounts on
    (user_id, phone). Counts are logged; secrets never are.
    """
    mode = str(os.environ.get('MIGRATE_SQLITE', '0')).strip().lower()
    if mode in ('0', 'false', 'no', 'off', ''):
        return {'skipped': True, 'reason': 'MIGRATE_SQLITE disabled'}
    if not DB_IS_POSTGRES:
        return {'skipped': True, 'reason': 'target database is already SQLite'}
    if not os.path.exists(_db_path):
        return {'skipped': True, 'reason': 'no legacy SQLite file found'}
    import sqlite3 as _sqlite3

    with app.app_context():
        if mode == 'auto' and User.query.count() > 0:
            return {'skipped': True, 'reason': 'target database already has users'}

        counts = {'users': 0, 'accounts': 0, 'settings': 0, 'proxies': 0, 'scheduled': 0}
        try:
            src = _sqlite3.connect(_db_path)
            src.row_factory = _sqlite3.Row
            cur = src.cursor()

            # Users (username is the identity key)
            try:
                for row in cur.execute('SELECT username, password, is_banned FROM user'):
                    if not row['username']:
                        continue
                    if User.query.filter_by(username=row['username']).first():
                        continue
                    db.session.add(User(
                        username=row['username'],
                        password=row['password'],
                        is_banned=bool(row['is_banned']) if 'is_banned' in row.keys() else False,
                    ))
                    counts['users'] += 1
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.warning('Legacy import: users skipped (%s)', e)

            # Telegram accounts (global identity is the phone number)
            try:
                for row in cur.execute('SELECT user_id, phone, session_string, is_active FROM telegram_account'):
                    if not row['phone'] or not row['session_string']:
                        continue
                    if TelegramAccount.query.filter_by(phone=row['phone']).first():
                        continue
                    owner = User.query.get(row['user_id']) if row['user_id'] else None
                    if owner is None:
                        owner = User.query.filter_by(username='admin').first()
                    if owner is None:
                        continue
                    db.session.add(TelegramAccount(
                        user_id=owner.id,
                        phone=row['phone'],
                        session_string=row['session_string'],
                        is_active=bool(row['is_active']) if row['is_active'] is not None else True,
                    ))
                    counts['accounts'] += 1
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.warning('Legacy import: accounts skipped (%s)', e)

            # API credentials
            try:
                for row in cur.execute('SELECT user_id, api_id, api_hash FROM settings'):
                    if not row['api_id'] or not row['api_hash']:
                        continue
                    owner = User.query.get(row['user_id']) if row['user_id'] else None
                    if owner is None or Settings.query.filter_by(user_id=owner.id).first():
                        continue
                    db.session.add(Settings(user_id=owner.id, api_id=str(row['api_id']), api_hash=str(row['api_hash'])))
                    counts['settings'] += 1
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.warning('Legacy import: settings skipped (%s)', e)

            # Proxies
            try:
                for row in cur.execute('SELECT user_id, proxy_type, host, port, username, password FROM proxy_config'):
                    owner = User.query.get(row['user_id']) if row['user_id'] else None
                    if owner is None or not row['host']:
                        continue
                    if ProxyConfig.query.filter_by(user_id=owner.id, host=row['host'], port=safe_int(row['port'], 0)).first():
                        continue
                    db.session.add(ProxyConfig(
                        user_id=owner.id,
                        proxy_type=clean_str(row['proxy_type'], 'socks5'),
                        host=row['host'], port=safe_int(row['port'], 0),
                        username=row['username'], password=row['password'],
                    ))
                    counts['proxies'] += 1
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.warning('Legacy import: proxies skipped (%s)', e)

            # Scheduled broadcasts (only future ones are still useful)
            try:
                for row in cur.execute('SELECT user_id, account_id, message_type, content, caption, groups, delay, scheduled_at, status FROM scheduled_broadcast'):
                    if not row['content'] or not row['groups']:
                        continue
                    owner = User.query.get(row['user_id']) if row['user_id'] else None
                    acct = TelegramAccount.query.get(row['account_id']) if row['account_id'] else None
                    if owner is None or acct is None:
                        continue
                    try:
                        when = datetime.fromisoformat(str(row['scheduled_at']))
                    except Exception:
                        continue
                    if ScheduledBroadcast.query.filter_by(user_id=owner.id, content=row['content'], scheduled_at=when).first():
                        continue
                    db.session.add(ScheduledBroadcast(
                        user_id=owner.id, account_id=acct.id,
                        message_type=clean_str(row['message_type'], 'text'),
                        content=row['content'], caption=row['caption'], groups=row['groups'],
                        delay=safe_int(row['delay'], 3), scheduled_at=when, status='pending',
                    ))
                    counts['scheduled'] += 1
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.warning('Legacy import: scheduled broadcasts skipped (%s)', e)

            src.close()
        except Exception as e:
            logger.error('Legacy SQLite import failed: %s', e)
            return {'skipped': True, 'reason': str(e)}
        logger.info('Legacy SQLite import finished: %s', counts)
        return {'skipped': False, 'counts': counts}


def resolve_secret_key():
    """Use SECRET_KEY from the environment, else generate one and persist it in the DB."""
    env_key = os.environ.get('SECRET_KEY') or os.environ.get('SESSION_SECRET')
    if env_key:
        app.config['SECRET_KEY'] = env_key
        return 'environment'
    try:
        if not GlobalSetting.get('app_secret_key'):
            GlobalSetting.set_val('app_secret_key', secrets.token_urlsafe(48))
            db.session.commit()
            logger.info('Generated a persistent SECRET_KEY and stored it in the database.')
        app.config['SECRET_KEY'] = GlobalSetting.get('app_secret_key')
        return 'database'
    except Exception as e:
        logger.warning('Could not persist SECRET_KEY (%s). Sessions will not survive a restart — set SECRET_KEY in the environment.', e)
        return 'ephemeral'


def ensure_default_admin():
    """Create the admin account, honouring the ADMIN_PASSWORD environment variable."""
    admin_password = os.environ.get('ADMIN_PASSWORD')
    admin_user = User.query.filter_by(username='admin').first()
    if not admin_user:
        if not admin_password:
            admin_password = 'MRADS@TG2026!'
            logger.warning(
                'ADMIN_PASSWORD is not set: the admin account was created with the legacy '
                'default password. Set ADMIN_PASSWORD in your environment and change it in '
                'the Admin panel.'
            )
        db.session.add(User(username='admin', password=generate_password_hash(admin_password)))
        db.session.commit()
        logger.info('Admin account created.')
        return 'created'
    if admin_password and not check_password_hash(admin_user.password, admin_password):
        # The environment variable is authoritative so a redeploy can reset the login.
        admin_user.password = generate_password_hash(admin_password)
        db.session.commit()
        logger.info('Admin password synced from the ADMIN_PASSWORD environment variable.')
        return 'synced'
    if check_password_hash(admin_user.password, 'admin123'):
        admin_user.password = generate_password_hash(admin_password or 'MRADS@TG2026!')
        db.session.commit()
        logger.warning('Legacy admin password "admin123" upgraded — change it in the Admin panel.')
        return 'upgraded'
    return 'unchanged'


def recover_interrupted_tasks():
    """Re-arm work that a restart interrupted, without ever duplicating a post."""
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=STALE_TASK_MINUTES)
        # 1. Durable campaign records left "running" by a killed process
        for row in TaskRun.query.filter(TaskRun.status == 'running', TaskRun.updated_at < cutoff).all():
            row.status = 'interrupted'
            note = 'Interrupted by an application restart'
            row.errors = f'{row.errors}\n{note}' if row.errors else note
            row.updated_at = datetime.utcnow()

        # 2. Auto Video Poster campaigns stuck mid-round ('posting' is never
        #    re-selected by the scheduler, so a crash used to stall it forever)
        now = datetime.utcnow()
        for campaign in AutoVideoPostCampaign.query.filter(
            AutoVideoPostCampaign.status.in_(['posting', 'running'])
        ).all():
            if campaign.source_type == 'zip':
                has_files = False
                try:
                    has_files = any(
                        os.path.splitext(name)[1].lower() in ALLOWED_VIDEO_EXTENSIONS
                        for name in os.listdir(campaign.media_dir)
                    )
                except OSError:
                    has_files = False
                if not has_files and _load_vault(campaign):
                    # Disk wiped, but the videos themselves are still on Telegram:
                    # keep posting from those copies instead of pausing.
                    has_files = True
                    campaign.needs_reupload = False
                if not has_files:
                    # Ephemeral Railway disk lost the extracted ZIP videos.
                    campaign.needs_reupload = True
                    campaign.status = 'paused'
                    campaign.last_error = (
                        'Uploaded ZIP media was lost (server restart/redeploy). '
                        'Re-create this campaign, or switch the source to Telegram Saved Messages.'
                    )
                    continue
            campaign.status = 'running'
            if campaign.next_run_at is None or campaign.next_run_at < now:
                campaign.next_run_at = now + timedelta(seconds=20)

        # 3. Scheduled broadcasts that were killed mid-run used to stay 'running'
        #    forever. Retry only when nothing was confirmed as sent.
        for sb in ScheduledBroadcast.query.filter(ScheduledBroadcast.status == 'running').all():
            if safe_int(getattr(sb, 'sent_count', 0), 0) > 0:
                sb.status = 'interrupted'
            else:
                sb.status = 'pending'
        db.session.commit()
    except Exception as e:
        logger.warning('recover_interrupted_tasks: %s', e)


def init_app_schema(import_legacy=False):
    """Create tables, migrate columns, persist the secret key, ensure the admin."""
    global DB_INIT_ERROR
    with app.app_context():
        try:
            db.create_all()
            run_schema_migration()
            DB_INIT_ERROR = None
        except Exception as e:
            DB_INIT_ERROR = str(e)
            logger.error(
                'Database initialization failed (%s). Check DATABASE_URL / Neon credentials. '
                'The dashboard will start but database-backed features are unavailable: %s',
                DB_SOURCE, e,
            )
            raise
        # Each startup step is isolated: a problem in one must not stop the others.
        for step_name, step in (('secret key', resolve_secret_key),
                                ('admin account', ensure_default_admin),
                                ('task recovery', recover_interrupted_tasks)):
            try:
                step()
            except Exception as e:
                logger.error('%s setup step failed: %s', step_name, e)
        if import_legacy:
            import_legacy_sqlite()
    logger.info('Database ready — backend=%s source=%s', 'postgresql' if DB_IS_POSTGRES else 'sqlite', DB_SOURCE)
    try:
        # Log the target (never the password) so a wrong URL is obvious in the logs.
        from sqlalchemy.engine import make_url
        target = make_url(DB_URI)
        logger.info('Database target: host=%s database=%s user=%s',
                    target.host, target.database, redact(target.username or ''))
    except Exception:
        pass


def start_background_jobs():
    """Start every scheduler exactly once per process."""
    if not BACKGROUND_JOBS_ENABLED:
        logger.info('BACKGROUND_JOBS_ENABLED=0 — scheduled jobs are disabled.')
        return
    start_job_once('daily_reset', schedule_daily_reset)
    # Runs the repeating rounds of every "Start Now" broadcast. Durable, so a
    # redeploy no longer kills a campaign that is waiting between rounds.
    start_job_once('broadcast_scheduler', run_broadcast_scheduler)
    start_job_once('scheduled_broadcasts', run_scheduled_broadcasts)
    start_job_once('health_monitor', schedule_health_monitor)
    start_job_once('channel_post_scheduler', run_channel_post_scheduler)
    start_job_once('auto_video_post_scheduler', run_auto_video_post_scheduler)
    start_job_once('scrape_dm_scheduler', run_scrape_dm_scheduler)
    start_job_once('auto_reply_bot', run_auto_reply_bot)
    start_job_once('member_alert_bot', run_member_alert_bot)
    start_job_once('generic_scheduler', run_generic_scheduler)


# Run schema initialization for `python main.py` AND for WSGI servers.
try:
    init_app_schema(import_legacy=True)
except Exception:
    pass  # a clear message was already logged; the app still starts and reports the error

if __name__ == '__main__':
    start_background_jobs()
    port = _env_int('PORT', 5000, 1, 65535)
    print(f"\n🚀 Telegram Automation Dashboard running at http://0.0.0.0:{port}")
    print(f"🗄️  Database: {'PostgreSQL/Neon' if DB_IS_POSTGRES else 'SQLite (local fallback)'} via {DB_SOURCE}")
    print("🔑 Login: admin / the ADMIN_PASSWORD you configured\n")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
