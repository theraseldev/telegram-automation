"""MR ADS TG AUTOMATION — automated test suite.

Everything here runs WITHOUT a live Telegram account or a live Railway/Neon
environment. Telegram is replaced by an in-process fake, so the tests verify the
application's own logic: request validation, campaign lifecycle, queue
bookkeeping, error handling and database persistence.

Run with:  python -m pytest tests/test_suite.py -v
"""

import asyncio
import io
import json
import os
import re
import shutil
import sys
import tempfile
import types
import zipfile
import pathlib

import pytest

# ── Environment must be configured BEFORE importing the application ─────────
TMP_ROOT = tempfile.mkdtemp(prefix='mr_ads_tg_tests_')
TEST_DB = os.path.join(TMP_ROOT, 'test.db').replace('\\', '/')
os.environ['DATABASE_URL'] = 'sqlite:///' + TEST_DB
os.environ['SECRET_KEY'] = 'test-secret-key'
os.environ['ADMIN_PASSWORD'] = 'test-admin-pass'
os.environ['BACKGROUND_JOBS_ENABLED'] = '0'   # no scheduler threads during tests
os.environ['ALLOW_REGISTRATION'] = '0'
os.environ['MIGRATE_SQLITE'] = '0'
# NOTE: no MAX_AUTO_POST_CHANNELS / MAX_UPLOAD_MB / delay overrides here — the
# tests must exercise the shipped defaults (which are effectively unlimited).

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import main  # noqa: E402  (import after env setup)

TEST_USER = 'tester'
TEST_USER_PASS = 'tester-pass'
ADMIN_PASS = os.environ['ADMIN_PASSWORD']


# ═══════════════════════════════════════════════════════════════════════════
# Fake Telegram layer
# ═══════════════════════════════════════════════════════════════════════════

class FakeFlood(Exception):
    """Stand-in for telethon FloodWaitError (module attribute is patched)."""

    def __init__(self, seconds=1):
        super().__init__('flood')
        self.seconds = seconds


class FakeEntity:
    def __init__(self, ident):
        self.ident = ident
        self.id = abs(hash(ident)) % 10_000
        self.username = str(ident).lstrip('@')
        self.title = str(ident)

    def __str__(self):
        return str(self.ident)


class FakeMessage:
    def __init__(self, msg_id, has_video=True, mime='video/mp4'):
        self.id = msg_id
        self.video = object() if has_video else None
        self.file = types.SimpleNamespace(mime_type=mime if has_video else 'image/png',
                                          name=f'video_{msg_id}.mp4')
        self.media = object() if has_video else None
        self.text = 'hi'
        self.message = 'hi'
        self.date = None
        self.out = True


class FakeTelegram:
    """Patch target for main.TelegramClient."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.sent = []                 # successful sends: (target, kind)
        self.send_attempts = []
        self.fail_send_for = set()     # target -> raise generic error
        self.fail_entity_for = set()   # target -> entity resolution fails
        self.flood_once_for = set()    # target -> raise FakeFlood on first attempt
        self.saved_messages = [FakeMessage(101), FakeMessage(102), FakeMessage(103)]
        self.messages_by_id = {}
        # What each confirmed send actually uploaded: 'upload' = bytes/path were
        # uploaded, 'telegram-copy' = an existing Telegram media object was re-used.
        self.file_payloads = []
        self.instances = []
        self.clients_connected = 0

    # factory used as a drop-in replacement for the TelegramClient class
    def client(self, *args, **kwargs):
        inst = _FakeClient(self)
        self.instances.append(inst)
        return inst


class _FakeClient:
    def __init__(self, harness):
        self.h = harness
        self.connected = False

    # ── connection ──────────────────────────────────────────────────────
    async def connect(self):
        self.connected = True
        self.h.clients_connected += 1

    async def disconnect(self):
        self.connected = False

    async def get_me(self):
        return types.SimpleNamespace(id=999, first_name='Test', last_name='User', username='tester')

    async def get_dialogs(self, limit=None):
        return []

    # ── entity resolution ───────────────────────────────────────────────
    async def get_entity(self, target):
        if target in self.h.fail_entity_for:
            raise ValueError('Cannot find any entity corresponding to the requested target')
        return FakeEntity(target)

    async def get_input_entity(self, target):
        return FakeEntity(target)

    # ── sending ─────────────────────────────────────────────────────────
    async def send_message(self, target, message, **kwargs):
        name = self._name(target)
        self.h.send_attempts.append((name, 'message'))
        if name in self.h.flood_once_for:
            self.h.flood_once_for.discard(name)
            raise FakeFlood(1)
        if name in self.h.fail_send_for:
            raise RuntimeError('send failed for ' + name)
        self.h.sent.append((name, 'message'))
        return FakeMessage(1)

    async def send_file(self, entity, file, caption=None, **kwargs):
        name = self._name(entity)
        self.h.send_attempts.append((name, 'file'))
        if name in self.h.flood_once_for:
            self.h.flood_once_for.discard(name)
            raise FakeFlood(1)
        if name in self.h.fail_send_for:
            raise RuntimeError('send failed for ' + name)
        # A file path must exist on disk; a Saved-Messages media object must be present.
        if isinstance(file, str):
            if not os.path.exists(file):
                raise FileNotFoundError(file)
            if os.path.getsize(file) == 0:
                raise RuntimeError('empty file')
        elif file is None:
            raise RuntimeError('no media')
        self.h.file_payloads.append((name, 'upload' if isinstance(file, str) else 'telegram-copy'))
        self.h.sent.append((name, 'file'))
        return FakeMessage(1)

    async def get_messages(self, peer, ids=None):
        if ids is None:
            return []
        if ids in self.h.messages_by_id:
            return self.h.messages_by_id[ids]
        return FakeMessage(ids)

    async def iter_messages(self, peer, limit=None):
        count = 0
        for msg in self.h.saved_messages:
            count += 1
            if limit and count > limit:
                break
            yield msg

    async def iter_dialogs(self):
        if False:  # pragma: no cover - keeps this an async generator
            yield None

    @staticmethod
    def _name(target):
        if isinstance(target, FakeEntity):
            return str(target.ident)
        return str(target)


def _fast_sleep(_seconds=0):
    """Replace asyncio.sleep so rate-limit waits do not slow the suite down."""
    return None


async def _fast_sleep_async(_seconds=0):
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope='session', autouse=True)
def _seed_user():
    with main.app.app_context():
        if not main.User.query.filter_by(username=TEST_USER).first():
            main.db.session.add(main.User(
                username=TEST_USER,
                password=main.generate_password_hash(TEST_USER_PASS),
            ))
            main.db.session.commit()


@pytest.fixture()
def app_client():
    # NOTE: no `with app.test_client()` here — holding an app context for the whole
    # test would nest with the app contexts the application itself pushes.
    main.app.config['TESTING'] = True
    client = main.app.test_client()
    yield client


@pytest.fixture()
def user_client(app_client):
    app_client.get('/logout')          # start clean so the login really switches user
    r = app_client.post('/login', data={'username': TEST_USER, 'password': TEST_USER_PASS})
    assert r.status_code in (302, 303), 'test user could not log in'
    return app_client


@pytest.fixture()
def admin_client(app_client):
    app_client.get('/logout')
    r = app_client.post('/login', data={'username': 'admin', 'password': ADMIN_PASS})
    assert r.status_code in (302, 303), 'admin could not log in'
    return app_client


@pytest.fixture()
def fake_tg(monkeypatch):
    harness = FakeTelegram()
    monkeypatch.setattr(main, 'TelegramClient', harness.client)
    monkeypatch.setattr(main, 'FloodWaitError', FakeFlood)
    # Telethon validates the session string format; tests use dummy session strings.
    monkeypatch.setattr(main, 'StringSession', lambda *args, **kwargs: 'fake-session')
    monkeypatch.setattr(main.asyncio, 'sleep', _fast_sleep_async)
    return harness


@pytest.fixture()
def account(user_client):
    """A connected Telegram account owned by the logged-in test user."""
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        acct = main.TelegramAccount(
            user_id=user.id,
            phone='+10000000001',
            session_string='FAKE-SESSION-STRING-FOR-TESTS',
            is_active=True,
        )
        main.db.session.add(acct)
        # the account owner needs API credentials for most flows
        if not main.Settings.query.filter_by(user_id=user.id).first():
            main.db.session.add(main.Settings(user_id=user.id, api_id='123456', api_hash='abcdef'))
        main.db.session.commit()
        yield acct.id


# ═══════════════════════════════════════════════════════════════════════════
# 1. Pure helpers (the reported NoneType crash class)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize('value,expected', [
    (None, 0), ('', 0), ('   ', 0), ('abc', 0), ([], 0), (None, 5),
])
def test_safe_int_never_raises(value, expected):
    # a default is supplied so the assertion is deterministic
    assert main.safe_int(value, expected) == expected


@pytest.mark.parametrize('value,expected', [
    ('7', 7), (7, 7), ('7.9', 7), (7.9, 7), ('  12 ', 12), (True, 0),
])
def test_safe_int_parses_numbers(value, expected):
    assert main.safe_int(value, 0) == expected


def test_safe_int_bounds():
    assert main.safe_int('999', 0, maximum=10) == 10
    assert main.safe_int('-5', 0, minimum=1) == 1


def test_safe_bool_and_clean_str():
    assert main.safe_bool('true') is True
    assert main.safe_bool('0') is False
    assert main.safe_bool(None, default=True) is True
    assert main.clean_str(None) == ''
    assert main.clean_str('  x  ') == 'x'


def test_parse_datetime_utc_handles_offsets_and_bad_input():
    naive = main.parse_datetime_utc('2030-01-02T03:04:05')
    assert naive is not None and naive.tzinfo is None
    zoned = main.parse_datetime_utc('2030-01-02T03:04:05Z')
    assert zoned is not None and zoned.tzinfo is None
    assert main.parse_datetime_utc('not-a-date') is None
    assert main.parse_datetime_utc('') is None
    assert main.parse_datetime_utc(None) is None


def test_dedupe_and_clean_list():
    assert main.dedupe_preserve(['a', 'b', 'a']) == ['a', 'b']
    assert main.clean_str_list('a\nb\na') == ['a', 'b']
    assert main.clean_str_list(['x', 'x', ' y ']) == ['x', 'y']
    assert main.clean_str_list(None) == []


@pytest.mark.parametrize('raw,kind,value', [
    ('@mychan', 'username', 'mychan'),
    ('mychan', 'username', 'mychan'),
    ('https://t.me/mychan', 'username', 'mychan'),
    ('t.me/mychan/123', 'username', 'mychan'),
    ('t.me/+AbCdEf123', 'invite', '+AbCdEf123'),
    ('https://t.me/joinchat/AbCdEf123', 'invite', '+AbCdEf123'),
    ('t.me/c/1234567890/42', 'private_post', '1234567890'),
    ('-1001234567890', 'id', '-1001234567890'),
])
def test_parse_channel_target_accepted(raw, kind, value):
    parsed = main.parse_channel_target(raw)
    assert parsed and parsed['kind'] == kind and parsed['value'] == value


@pytest.mark.parametrize('raw', ['', None, '   ', 'bad name!!', 't.me/c/notanumber/1', 'invite with space'])
def test_parse_channel_target_rejected(raw):
    assert main.parse_channel_target(raw) in (None, '')


def test_friendly_telegram_error_messages(monkeypatch):
    monkeypatch.setattr(main, 'FloodWaitError', FakeFlood)
    assert 'rate limit' in main.friendly_telegram_error(FakeFlood(42)).lower()
    assert 'not an admin' in main.friendly_telegram_error(main.ChatAdminRequiredError(None)).lower()
    assert 'private' in main.friendly_telegram_error(main.ChannelPrivateError(None)).lower()


@pytest.mark.parametrize('interval,count,expected', [
    (0, 0, (False, 0)),      # no interval -> never repeat
    (0, 5, (False, 0)),
    (6, 0, (True, 0)),       # 0 = infinite (UI label "0 = ∞")
    (6, 1, (False, 0)),      # 1 run in total -> no repeat
    (6, 2, (True, 1)),       # 2 runs in total -> one repeat left
    (6, 3, (True, 2)),
])
def test_next_repeat_settings(interval, count, expected):
    assert main.next_repeat_settings(interval, count) == expected


def test_redact_hides_secrets():
    out = main.redact('1234567890abcdef')
    assert '7890abcdef' not in out
    assert len(main.redact('')) == 0


# ═══════════════════════════════════════════════════════════════════════════
# 2. Template / route contract
# ═══════════════════════════════════════════════════════════════════════════

def _read_template(name):
    return (PROJECT_ROOT / 'templates' / name).read_text(encoding='utf-8')


def _frontend_endpoints(html):
    return {m for m in re.findall(r"['\"](/(?:bot|api)/[A-Za-z0-9_\-/]*)", html) if m != '/'}


def _known_rule_prefixes():
    prefixes = set()
    for rule in main.app.url_map.iter_rules():
        rule_text = str(rule)
        prefixes.add(rule_text)
        prefixes.add(rule_text.split('<')[0].rstrip('/'))
    return prefixes


def test_every_dashboard_endpoint_exists_in_backend():
    prefixes = _known_rule_prefixes()
    endpoints = _frontend_endpoints(_read_template('dashboard.html'))
    assert len(endpoints) > 50, 'dashboard contract extraction looks wrong'
    missing = []
    for endpoint in sorted(endpoints):
        probe = endpoint.rstrip('/')
        if probe in prefixes:
            continue
        if any(probe.startswith(pre) and pre for pre in prefixes if pre.startswith('/bot') or pre.startswith('/api')):
            continue
        missing.append(endpoint)
    assert missing == [], 'frontend calls endpoints the backend does not serve: %s' % missing


def test_admin_panel_calls_only_existing_endpoints():
    html = _read_template('admin.html')
    # Regression: the panel used to call /api/admin/* which does not exist.
    assert "fetch('/api/admin" not in html and '"' + '/api/admin' + '"' not in html, \
        'admin.html still calls the non-existent /api/admin/* routes'
    assert "ADMIN_API = '/bot/admin'" in html
    prefixes = _known_rule_prefixes()
    missing = []
    for endpoint in sorted(_frontend_endpoints(html)):
        probe = endpoint.rstrip('/')
        if probe in prefixes or any(probe.startswith(pre) and pre for pre in prefixes):
            continue
        missing.append(endpoint)
    assert missing == [], 'admin panel calls missing endpoints: %s' % missing


def test_dashboard_has_no_duplicate_element_ids():
    html = _read_template('dashboard.html')
    ids = re.findall(r'\bid="([^"]+)"', html)
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    assert duplicates == [], 'duplicate element ids break getElementById: %s' % duplicates


def test_avp_channel_limit_text_is_not_hardcoded_wrongly():
    html = _read_template('dashboard.html')
    # The label must come from the shared limit, not a stale hardcoded number.
    assert 'id="avpChannelLimit"' in html
    assert 'max 10)' not in html


# ═══════════════════════════════════════════════════════════════════════════
# 3. Auth and access control
# ═══════════════════════════════════════════════════════════════════════════

def test_api_requires_login(app_client):
    r = app_client.get('/bot/dashboard_stats')
    assert r.status_code == 401
    assert r.is_json and r.get_json()['login_required'] is True


def test_public_registration_is_disabled(app_client):
    r = app_client.post('/register', data={'username': 'sneaky', 'password': 'secret123'})
    assert r.status_code == 403
    assert r.get_json()['registration_disabled'] is True


def test_login_rejects_bad_password(app_client):
    r = app_client.post('/login', data={'username': TEST_USER, 'password': 'wrong'})
    assert r.status_code == 401


def test_banned_user_cannot_log_in(app_client):
    with main.app.app_context():
        u = main.User(username='banned_user', password=main.generate_password_hash('pw12345'), is_banned=True)
        main.db.session.add(u)
        main.db.session.commit()
    r = app_client.post('/login', data={'username': 'banned_user', 'password': 'pw12345'})
    assert r.status_code == 403


def test_admin_routes_reject_normal_user(user_client):
    r = user_client.get('/bot/admin/users')
    assert r.status_code in (401, 403)


def test_db_health_reports_backend_without_secrets(user_client):
    r = user_client.get('/bot/db_health')
    assert r.status_code == 200
    payload = r.get_json()
    assert payload['connected'] is True
    assert payload['backend'] in ('sqlite', 'postgresql')
    body = json.dumps(payload).lower()
    for secret in ('password', 'api_hash', 'session'):
        assert secret not in body


def test_app_config_exposes_shared_limits(user_client):
    payload = user_client.get('/bot/app_config').get_json()
    assert payload['max_auto_post_channels'] == main.MAX_AUTO_POST_CHANNELS
    assert payload['auto_post_delay_min'] == 1
    assert payload['auto_post_delay_max'] == main.AUTO_POST_DELAY_MAX


# ═══════════════════════════════════════════════════════════════════════════
# 4. Regression: optional/empty input must answer 4xx, never crash with 500
# ═══════════════════════════════════════════════════════════════════════════

EMPTY_BODY_ENDPOINTS = [
    '/bot/broadcast',
    '/bot/schedule_broadcast',
    '/bot/send_dm',
    '/bot/send_dm_multi',
    '/bot/dm_campaign',
    '/bot/send_personal_message',
    '/bot/forward_message',
    '/bot/forward_messages',
    '/bot/get_chat_history',
    '/bot/channel_post/create',
]


@pytest.mark.parametrize('endpoint', EMPTY_BODY_ENDPOINTS)
def test_empty_json_body_is_a_validation_error(user_client, endpoint):
    r = user_client.post(endpoint, json={})
    assert r.status_code < 500, '%s crashed on an empty body: %s' % (endpoint, r.data[:200])
    assert r.get_json()['success'] is False


@pytest.mark.parametrize('endpoint', EMPTY_BODY_ENDPOINTS)
def test_none_values_are_a_validation_error(user_client, endpoint):
    r = user_client.post(endpoint, json={'account_id': None, 'delay': None, 'limit': None,
                                         'group_ids': None, 'usernames': None, 'targets': None})
    assert r.status_code < 500, '%s crashed on None values: %s' % (endpoint, r.data[:200])


def test_auto_video_create_without_channels(user_client, account):
    r = user_client.post('/bot/auto_video_post/create', data={'account_id': str(account)})
    assert r.status_code == 400
    assert 'channel' in r.get_json()['error'].lower()


def test_unknown_api_route_returns_json_404(user_client):
    r = user_client.get('/bot/this-route-does-not-exist')
    assert r.status_code == 404
    assert r.is_json


def test_unexpected_error_handler_returns_json(monkeypatch, user_client):
    monkeypatch.setattr(main, 'get_accounts_for_current_user',
                        lambda: (_ for _ in ()).throw(RuntimeError('boom')))
    r = user_client.get('/bot/dashboard_stats')
    assert r.status_code == 500
    assert r.is_json and r.get_json()['success'] is False


# ═══════════════════════════════════════════════════════════════════════════
# 5. Auto Video Poster
# ═══════════════════════════════════════════════════════════════════════════

def _create_saved_campaign(client, account_id, channels, min_delay=1, max_delay=1, caption='cap {index}'):
    return client.post('/bot/auto_video_post/create', data={
        'account_id': str(account_id),
        'source_type': 'saved_messages',
        'channels': '\n'.join(channels),
        'caption': caption,
        'min_delay': str(min_delay),
        'max_delay': str(max_delay),
    })


def test_limit_defaults_are_not_restrictive():
    """Defaults must not cap the operator — every ceiling is env-configurable."""
    assert main.MAX_AUTO_POST_CHANNELS >= 100000
    assert main.MAX_DELAY_SECONDS >= 604800        # >= 7 days
    assert main.AUTO_POST_DELAY_MAX >= 10080
    assert main.MAX_FORWARD_MESSAGES >= 100000
    assert main.MAX_HISTORY_LIMIT >= 100000
    assert main.MAX_LIST_ROWS >= 100000
    assert main.MAX_BULK_COUNT >= 1000000
    assert main.MAX_SCRAPE_LIMIT >= 1000000
    assert main.MAX_JOIN_DAILY_LIMIT >= 1000000
    assert main.MAX_SCHEDULED_BATCH >= 5000
    assert main.MAX_FLOOD_WAIT_SECONDS >= 86400
    assert main.MAX_UPLOAD_MB == 0 or main.MAX_UPLOAD_MB >= 4096


@pytest.mark.parametrize('channel_count', [1, 10, 11, 50, 51, 120])
def test_many_channels_are_accepted_by_default(user_client, account, fake_tg, channel_count):
    channels = ['chan%d' % i for i in range(channel_count)]
    r = _create_saved_campaign(user_client, account, channels)
    assert r.status_code == 200, r.data[:400]
    assert r.get_json()['channels'] == channel_count


def test_configured_channel_limit_is_enforced_when_the_operator_sets_one(monkeypatch, user_client,
                                                                        account, fake_tg):
    monkeypatch.setattr(main, 'MAX_AUTO_POST_CHANNELS', 3)
    r = _create_saved_campaign(user_client, account, ['chan1', 'chan2', 'chan3', 'chan4'])
    assert r.status_code == 400
    assert '3' in r.get_json()['error']


def test_zero_means_no_limit_everywhere(monkeypatch):
    """Setting a ceiling to 0 in Railway variables must free it, not clamp to 0."""
    monkeypatch.setenv('TEST_LIMIT_ZERO', '0')
    monkeypatch.setenv('TEST_LIMIT_POSITIVE', '25')
    assert main._limit_env('TEST_LIMIT_ZERO', 7) == main.UNLIMITED
    assert main._limit_env('TEST_LIMIT_POSITIVE', 7) == 25
    monkeypatch.delenv('TEST_LIMIT_ZERO', raising=False)
    assert main._limit_env('TEST_LIMIT_ZERO', 7) == 7          # unset -> shipped default


def test_channel_limit_zero_allows_any_number(monkeypatch, user_client, account, fake_tg):
    monkeypatch.setattr(main, 'MAX_AUTO_POST_CHANNELS', 0)     # 0 = unlimited
    channels = ['chan%d' % i for i in range(60)]
    r = _create_saved_campaign(user_client, account, channels)
    assert r.status_code == 200, r.data[:200]


def test_upload_limit_zero_removes_request_cap():
    from flask import Flask
    assert main.MAX_UPLOAD_MB == 0 or main.MAX_UPLOAD_MB >= 4096
    # the effective Flask cap mirrors the setting
    expected = None if main.MAX_UPLOAD_MB == 0 else main.MAX_UPLOAD_MB * 1024 * 1024
    assert main.app.config['MAX_CONTENT_LENGTH'] == expected


def test_delay_accepts_large_values(user_client, account, fake_tg):
    r = user_client.post('/bot/broadcast', json={
        'account_id': account, 'content': 'x', 'group_ids': ['grp1'], 'delay': 50000,
    })
    assert r.status_code == 200, r.data[:300]
    with main.app.app_context():
        task = main.BroadcastTask.query.order_by(main.BroadcastTask.id.desc()).first()
        assert task.delay == 50000          # not silently clamped to 3600


def test_large_forward_count_is_not_clamped(user_client, account, fake_tg):
    r = user_client.post('/bot/forward_messages', json={
        'account_id': account, 'source_chat': 'src', 'target_chats': 'dst1',
        'msg_count': 5000, 'delay': 1,
    })
    assert r.status_code == 200, r.data[:300]


def test_app_config_reports_the_effective_ceilings(user_client):
    cfg = user_client.get('/bot/app_config').get_json()
    assert cfg['max_auto_post_channels'] == main.MAX_AUTO_POST_CHANNELS
    assert cfg['max_delay_seconds'] == main.MAX_DELAY_SECONDS
    assert cfg['max_forward_messages'] == main.MAX_FORWARD_MESSAGES
    assert cfg['max_scrape_limit'] == main.MAX_SCRAPE_LIMIT


def test_saved_messages_campaign_scans_and_persists_message_ids(user_client, account, fake_tg):
    r = _create_saved_campaign(user_client, account, ['chan1', 'chan2'])
    assert r.status_code == 200, r.data[:300]
    payload = r.get_json()
    assert payload['videos'] == 3 and payload['channels'] == 2
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(payload['campaign_id'])
        assert json.loads(campaign.source_ids) == [101, 102, 103]
        assert campaign.source_type == 'saved_messages'
        # queue state is stored in the database, not only on the ephemeral disk
        assert json.loads(campaign.queue_state)['version'] == 3
        assert set(json.loads(campaign.queue_state)['channels']) == {'chan1', 'chan2'}


def test_invalid_channel_is_rejected(user_client, account, fake_tg):
    r = _create_saved_campaign(user_client, account, ['bad name!!'])
    assert r.status_code == 400
    assert 'invalid' in r.get_json()['error'].lower()


def test_zip_source_requires_a_file(user_client, account, fake_tg):
    r = user_client.post('/bot/auto_video_post/create', data={
        'account_id': str(account), 'source_type': 'zip', 'channels': 'chan1',
    })
    assert r.status_code == 400
    assert 'zip' in r.get_json()['error'].lower()


def test_zip_without_videos_is_rejected(user_client, account, fake_tg):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as zf:
        zf.writestr('notes.txt', 'no videos here')
    buffer.seek(0)
    r = user_client.post('/bot/auto_video_post/create', data={
        'account_id': str(account), 'source_type': 'zip', 'channels': 'chan1',
        'zip_files': (buffer, 'empty.zip'),
    }, content_type='multipart/form-data')
    assert r.status_code == 400
    assert 'already' in r.get_json()['error'] or 'video' in r.get_json()['error'].lower()


def _set_queues(campaign_id, state):
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(campaign_id)
        campaign.queue_state = json.dumps(state)
        main.db.session.commit()


def _campaign_state(campaign_id):
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(campaign_id)
        return {
            'status': campaign.status,
            'posted': campaign.posted_count,
            'failed': campaign.failed_count,
            'skipped': campaign.skipped_count,
            'rounds': campaign.rounds_completed,
            'current_index': campaign.current_index,
            'last_error': campaign.last_error,
            'summary': campaign.last_round_summary,
            'next_run_at': campaign.next_run_at,
            'queues': json.loads(campaign.queue_state or '{}'),
            'posted_at': campaign.last_post_at,
        }


def test_round_posts_to_every_channel_and_advances_queues(user_client, account, fake_tg):
    payload = _create_saved_campaign(user_client, account, ['chan1', 'chan2']).get_json()
    _set_queues(payload['campaign_id'], {
        'version': 3,
        'channels': {
            'chan1': {'order': [0, 1, 2], 'position': 0, 'cycle': 1},
            'chan2': {'order': [1, 2, 0], 'position': 0, 'cycle': 1},
        },
    })
    main._run_auto_post_round(payload['campaign_id'])
    state = _campaign_state(payload['campaign_id'])
    assert state['posted'] == 2, state
    assert state['failed'] == 0 and state['skipped'] == 0
    assert state['rounds'] == 1
    assert state['queues']['channels']['chan1']['position'] == 1
    assert state['queues']['channels']['chan2']['position'] == 1
    assert state['next_run_at'] is not None
    assert state['posted_at'] is not None
    assert len([s for s in fake_tg.sent if s[1] == 'file']) == 2


def test_round_uses_different_videos_for_different_channels(user_client, account, fake_tg):
    payload = _create_saved_campaign(user_client, account, ['chan1', 'chan2']).get_json()
    _set_queues(payload['campaign_id'], {
        'version': 3,
        'channels': {
            'chan1': {'order': [0, 1, 2], 'position': 0, 'cycle': 1},
            'chan2': {'order': [0, 1, 2], 'position': 0, 'cycle': 1},
        },
    })
    main._run_auto_post_round(payload['campaign_id'])
    state = _campaign_state(payload['campaign_id'])
    assert state['posted'] == 2
    positions = [q['order'][0] for q in state['queues']['channels'].values()]
    assert positions[0] != positions[1], 'channels were given the same video in one round'


def test_failed_channel_does_not_block_other_channels(user_client, account, fake_tg):
    fake_tg.fail_send_for = {'chan2'}
    payload = _create_saved_campaign(user_client, account, ['chan1', 'chan2', 'chan3']).get_json()
    _set_queues(payload['campaign_id'], {
        'version': 3,
        'channels': {
            'chan1': {'order': [0, 1, 2], 'position': 0, 'cycle': 1},
            'chan2': {'order': [1, 2, 0], 'position': 0, 'cycle': 1},
            'chan3': {'order': [2, 0, 1], 'position': 0, 'cycle': 1},
        },
    })
    main._run_auto_post_round(payload['campaign_id'])
    state = _campaign_state(payload['campaign_id'])
    assert state['posted'] == 2 and state['failed'] == 1
    assert 'chan2' in (state['last_error'] or '')
    # the failing channel must not advance its queue (it will retry next round)
    assert state['queues']['channels']['chan2']['position'] == 0
    assert state['status'] == 'running'


def test_dead_saved_message_skips_only_that_video(user_client, account, fake_tg):
    fake_tg.messages_by_id[101] = None   # first queued video no longer exists
    payload = _create_saved_campaign(user_client, account, ['chan1', 'chan2', 'chan3']).get_json()
    _set_queues(payload['campaign_id'], {
        'version': 3,
        'channels': {
            'chan1': {'order': [0, 1, 2], 'position': 0, 'cycle': 1},
            'chan2': {'order': [1, 2, 0], 'position': 0, 'cycle': 1},
            'chan3': {'order': [2, 0, 1], 'position': 0, 'cycle': 1},
        },
    })
    main._run_auto_post_round(payload['campaign_id'])
    state = _campaign_state(payload['campaign_id'])
    assert state['posted'] == 2 and state['skipped'] == 1, state
    assert 'no longer available' in (state['last_error'] or '')
    # the dead video is skipped for that channel only, so it is not retried forever
    assert state['queues']['channels']['chan1']['position'] == 1
    assert state['status'] == 'running'


def test_flood_wait_is_honoured_then_retried(user_client, account, fake_tg):
    fake_tg.flood_once_for = {'chan1'}
    payload = _create_saved_campaign(user_client, account, ['chan1']).get_json()
    _set_queues(payload['campaign_id'], {
        'version': 3, 'channels': {'chan1': {'order': [0, 1, 2], 'position': 0, 'cycle': 1}},
    })
    main._run_auto_post_round(payload['campaign_id'])
    state = _campaign_state(payload['campaign_id'])
    assert state['posted'] == 1 and state['skipped'] == 0, state
    assert 'rate limit' in (state['last_error'] or '').lower()


def test_missing_zip_media_marks_campaign_for_reupload(user_client, account, fake_tg):
    payload = _create_saved_campaign(user_client, account, ['chan1']).get_json()
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(payload['campaign_id'])
        campaign.source_type = 'zip'          # pretend it was a ZIP campaign
        campaign.source_ids = None
        main.db.session.commit()
    main._run_auto_post_round(payload['campaign_id'])
    state = _campaign_state(payload['campaign_id'])
    assert state['status'] == 'paused'
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(payload['campaign_id'])
        assert campaign.needs_reupload is True
        assert 'media' in (campaign.last_error or '').lower()


def test_pause_resume_delete_lifecycle(user_client, account, fake_tg):
    payload = _create_saved_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']

    assert user_client.post('/bot/auto_video_post/%d/pause' % campaign_id).status_code == 200
    assert _campaign_state(campaign_id)['status'] == 'paused'

    assert user_client.post('/bot/auto_video_post/%d/resume' % campaign_id).status_code == 200
    assert _campaign_state(campaign_id)['status'] == 'running'

    listed = user_client.get('/bot/auto_video_post/list').get_json()
    assert listed['max_channels'] == main.MAX_AUTO_POST_CHANNELS
    row = [c for c in listed['campaigns'] if c['id'] == campaign_id][0]
    for key in ('posted_count', 'failed_count', 'skipped_count', 'rounds_completed',
                'available_videos', 'needs_reupload', 'channel_count'):
        assert key in row, 'campaign list is missing %s' % key

    assert user_client.delete('/bot/auto_video_post/%d' % campaign_id).status_code == 200
    with main.app.app_context():
        assert main.AutoVideoPostCampaign.query.get(campaign_id) is None


def test_pause_resume_when_source_is_gone(user_client, account, fake_tg):
    payload = _create_saved_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(campaign_id)
        campaign.source_ids = '[]'
        main.db.session.commit()
    r = user_client.post('/bot/auto_video_post/%d/resume' % campaign_id)
    assert r.status_code == 400
    assert 'empty' in r.get_json()['error'].lower()


def test_zip_extraction_keeps_unicode_and_skips_empty(tmp_path):
    archive = tmp_path / 'videos.zip'
    with zipfile.ZipFile(archive, 'w') as zf:
        zf.writestr('ভিডিও.mp4', b'x' * 32)
        zf.writestr('empty.mp4', b'')
        zf.writestr('../../escape.mp4', b'x' * 32)
        zf.writestr('notes.txt', 'skip me')
    destination = tmp_path / 'out'
    extracted, skipped = main._safe_extract_videos(str(archive), str(destination))
    names = sorted(os.path.basename(p) for p in extracted)
    assert 'ভিডিও.mp4' in names
    assert 'empty.mp4' not in names
    assert all(os.path.abspath(p).startswith(str(destination)) for p in extracted)
    assert 'empty.mp4' in skipped


def test_recover_interrupted_tasks_resets_stuck_campaigns(user_client, account, fake_tg, tmp_path):
    empty_dir = tmp_path / 'lost_media'
    empty_dir.mkdir()
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        stuck = main.AutoVideoPostCampaign(
            user_id=user.id, account_id=account, channels=json.dumps(['chan1']),
            media_dir=str(empty_dir), total_videos=1, status='posting',
            source_type='zip', posted_count=0,
        )
        main.db.session.add(stuck)
        main.db.session.commit()
        stuck_id = stuck.id
        main.recover_interrupted_tasks()
        refreshed = main.AutoVideoPostCampaign.query.get(stuck_id)
        assert refreshed.status == 'paused'
        assert refreshed.needs_reupload is True


# ═══════════════════════════════════════════════════════════════════════════
# 6. Messaging campaigns
# ═══════════════════════════════════════════════════════════════════════════

def _wait_for(predicate, timeout=15.0):
    import time as _time
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        with main.app.app_context():
            if predicate():
                return True
        _time.sleep(0.05)
    return False


def _broadcast_task(task_id):
    with main.app.app_context():
        task = main.BroadcastTask.query.get(task_id)
        return None if task is None else {
            'status': task.status, 'sent': task.sent_count, 'account_id': task.account_id,
        }


def _broadcast(client, account_id, groups, content='hello'):
    return client.post('/bot/broadcast', json={
        'account_id': account_id, 'content': content, 'group_ids': groups, 'delay': 1,
    })


def test_broadcast_success_reports_honest_counters(user_client, account, fake_tg):
    r = _broadcast(user_client, account, ['grp1', 'grp2'])
    assert r.status_code == 200, r.data[:300]
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_task(task_id) or {}).get('status') != 'running')
    task = _broadcast_task(task_id)
    assert task['status'] == 'completed'
    assert task['sent'] == 2
    progress = user_client.get('/bot/task_status/%d' % task_id).get_json()['progress']
    assert progress['sent'] == 2 and progress['failed'] == 0
    assert progress.get('persisted') is True


def test_broadcast_partial_failure_is_not_reported_as_success(user_client, account, fake_tg):
    fake_tg.fail_send_for = {'grp2'}
    r = _broadcast(user_client, account, ['grp1', 'grp2'])
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_task(task_id) or {}).get('status') != 'running')
    task = _broadcast_task(task_id)
    assert task['status'] == 'partial', task
    assert task['sent'] == 1
    progress = user_client.get('/bot/task_status/%d' % task_id).get_json()['progress']
    assert progress['failed'] == 1
    assert progress['errors'], 'per-target errors must be reported'


def test_broadcast_total_failure_is_reported_as_failed(user_client, account, fake_tg):
    fake_tg.fail_send_for = {'grp1', 'grp2'}
    r = _broadcast(user_client, account, ['grp1', 'grp2'])
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_task(task_id) or {}).get('status') != 'running')
    assert _broadcast_task(task_id)['status'] == 'failed'


def test_broadcast_honours_flood_wait_and_reports_skipped(user_client, account, fake_tg):
    fake_tg.flood_once_for = {'grp1'}
    r = _broadcast(user_client, account, ['grp1', 'grp2'])
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_task(task_id) or {}).get('status') != 'running')
    progress = user_client.get('/bot/task_status/%d' % task_id).get_json()['progress']
    assert progress['skipped'] == 1, progress
    assert progress['sent'] == 1, progress
    assert any('rate limit' in e.lower() for e in progress['errors'])


def test_broadcast_deduplicates_recipients(user_client, account, fake_tg):
    r = _broadcast(user_client, account, ['grp1', 'grp1', 'grp2'])
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_task(task_id) or {}).get('status') != 'running')
    assert _broadcast_task(task_id)['sent'] == 2
    assert len([s for s in fake_tg.sent if s[0] == 'grp1']) == 1, 'duplicate target received twice'


def test_second_campaign_on_a_busy_account_is_rejected(user_client, account, fake_tg):
    lock = main.get_account_lock(account)
    lock.acquire()
    try:
        r = _broadcast(user_client, account, ['grp1'])
        assert r.status_code == 409
        assert 'already' in r.get_json()['error'].lower()
    finally:
        lock.release()


def test_progress_survives_process_memory_loss(user_client, account, fake_tg):
    r = _broadcast(user_client, account, ['grp1'])
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_task(task_id) or {}).get('status') != 'running')
    main.task_progress.clear()          # simulate a restart losing in-memory state
    progress = user_client.get('/bot/task_status/%d' % task_id).get_json()['progress']
    assert progress['sent'] == 1 and progress['status'] == 'completed'


def test_dm_campaign_deduplicates_and_persists_progress(user_client, account, fake_tg):
    r = user_client.post('/bot/dm_campaign', json={
        'account_id': account, 'delay': 1, 'personalize': True,
        'targets': [{'username': 'u1', 'first_name': 'Ann'},
                    {'username': 'u2', 'first_name': 'Bob'},
                    {'username': '@u1', 'first_name': 'Ann'}],
        'messages': ['hi {name}'],
    })
    assert r.status_code == 200, r.data[:300]
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (main.task_run_snapshot(task_id) or {}).get('status') != 'running')
    progress = user_client.get('/bot/campaign_status/%s' % task_id).get_json()['progress']
    assert progress['sent'] == 2, progress       # the duplicate target was removed
    assert progress['status'] == 'completed'
    assert len(fake_tg.sent) == 2


def test_dm_campaign_reports_failures_per_target(user_client, account, fake_tg):
    fake_tg.fail_send_for = {'u2'}
    r = user_client.post('/bot/dm_campaign', json={
        'account_id': account, 'delay': 1,
        'targets': [{'username': 'u1'}, {'username': 'u2'}],
        'messages': ['hi'],
    })
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (main.task_run_snapshot(task_id) or {}).get('status') != 'running')
    progress = user_client.get('/bot/campaign_status/%s' % task_id).get_json()['progress']
    assert progress['sent'] == 1 and progress['failed'] == 1
    assert progress['status'] == 'partial'


def test_multi_account_dm_rejects_unusable_accounts(user_client, account, fake_tg):
    r = user_client.post('/bot/send_dm_multi', json={
        'account_ids': [999999], 'usernames': ['u1'], 'message': 'hi',
    })
    assert r.status_code == 400
    assert 'no usable account' in r.get_json()['error'].lower()


def test_multi_account_dm_splits_and_reports_progress(user_client, account, fake_tg):
    r = user_client.post('/bot/send_dm_multi', json={
        'account_ids': [account], 'usernames': ['u1', 'u2'], 'message': 'hi', 'delay': 1,
    })
    assert r.status_code == 200, r.data[:300]
    task_key = r.get_json()['task_key']
    assert _wait_for(lambda: (main.task_run_snapshot(task_key) or {}).get('sent') == 2)
    snapshot = main.task_run_snapshot(task_key)
    assert snapshot['sent'] == 2


def test_dm_campaign_missing_account_is_validation_error(user_client, fake_tg):
    r = user_client.post('/bot/dm_campaign', json={'targets': [{'username': 'u1'}], 'messages': ['hi']})
    assert r.status_code == 400
    assert 'account' in r.get_json()['error'].lower()


# ═══════════════════════════════════════════════════════════════════════════
# 7. Scheduled broadcasting
# ═══════════════════════════════════════════════════════════════════════════

def test_schedule_broadcast_validation(user_client, account, fake_tg):
    base = {'account_id': account, 'content': 'x', 'group_ids': ['grp1']}

    assert user_client.post('/bot/schedule_broadcast', json={}).status_code == 400
    assert user_client.post('/bot/schedule_broadcast',
                            json=dict(base, scheduled_at='not-a-date')).status_code == 400
    assert user_client.post('/bot/schedule_broadcast',
                            json=dict(base, scheduled_at='2000-01-01T00:00:00')).status_code == 400
    assert user_client.post('/bot/schedule_broadcast', json=dict(base)).status_code == 400


def test_schedule_broadcast_accepts_utc_suffix_and_bad_repeat_values(user_client, account, fake_tg):
    r = user_client.post('/bot/schedule_broadcast', json={
        'account_id': account, 'content': 'hello', 'group_ids': ['grp1', 'grp1'],
        'scheduled_at': '2031-05-05T10:00:00Z', 'repeat_interval': 'abc', 'repeat_count': '',
    })
    assert r.status_code == 200, r.data[:300]
    sb_id = r.get_json()['id']
    with main.app.app_context():
        row = main.ScheduledBroadcast.query.get(sb_id)
        assert row.status == 'pending'
        assert json.loads(row.groups) == ['grp1']      # duplicates removed
        assert row.repeat_interval == 0 and row.repeat_count == 0


def test_scheduled_list_survives_corrupt_groups_value(user_client, account, fake_tg):
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        bad = main.ScheduledBroadcast(
            user_id=user.id, account_id=account, content='x', groups='not-json',
            delay=1, scheduled_at=main.datetime.utcnow() + main.timedelta(hours=1),
        )
        main.db.session.add(bad)
        main.db.session.commit()
        bad_id = bad.id
    r = user_client.get('/bot/get_scheduled_broadcasts')
    assert r.status_code == 200
    row = [s for s in r.get_json()['scheduled'] if s['id'] == bad_id]
    assert row and row[0]['groups_count'] == 0


def test_recover_interrupted_tasks_requeues_scheduled_broadcasts(user_client, account, fake_tg):
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        stuck = main.ScheduledBroadcast(
            user_id=user.id, account_id=account, content='x', groups=json.dumps(['grp1']),
            delay=1, scheduled_at=main.datetime.utcnow() - main.timedelta(minutes=5),
            status='running', sent_count=0,
        )
        partial = main.ScheduledBroadcast(
            user_id=user.id, account_id=account, content='y', groups=json.dumps(['grp1']),
            delay=1, scheduled_at=main.datetime.utcnow() - main.timedelta(minutes=5),
            status='running', sent_count=2,
        )
        main.db.session.add_all([stuck, partial])
        main.db.session.commit()
        stuck_id, partial_id = stuck.id, partial.id
        main.recover_interrupted_tasks()
        assert main.ScheduledBroadcast.query.get(stuck_id).status == 'pending'
        assert main.ScheduledBroadcast.query.get(partial_id).status == 'interrupted'


# ═══════════════════════════════════════════════════════════════════════════
# 8. Database, migration and platform behaviour
# ═══════════════════════════════════════════════════════════════════════════

def test_database_uri_normalisation():
    assert main._normalize_db_url('postgres://u:p@h/db').startswith('postgresql+psycopg2://')
    assert 'sslmode=require' in main._normalize_db_url('postgresql://u:p@h/db')
    assert main._normalize_db_url('postgresql://u:p@h/db?sslmode=disable').count('sslmode') == 1
    assert main._normalize_db_url('sqlite:///x.db') == 'sqlite:///x.db'
    assert main._normalize_db_url('') == ''


def test_neon_channel_binding_is_removed():
    """Neon console URLs carry channel_binding=require, which older libpq rejects
    and which made every database call fail (so /login answered 500)."""
    url = main._normalize_db_url(
        'postgresql://u:p@ep-x-pooler.neon.tech/neondb?sslmode=require&channel_binding=require')
    assert 'channel_binding' not in url
    assert 'sslmode=require' in url
    assert url.endswith('sslmode=require')
    # a URL whose only parameter was channel_binding must not keep a dangling '?'
    assert main._normalize_db_url('postgresql://u:p@h/db?channel_binding=require').endswith('/db?sslmode=require')


def test_session_loader_never_raises(monkeypatch):
    """A broken database or cookie must not turn every page into HTTP 500."""
    class BrokenQuery:
        @staticmethod
        def get(_ident):
            raise RuntimeError('database down')

    class BrokenUser:
        query = BrokenQuery()

    monkeypatch.setattr(main, 'User', BrokenUser)
    assert main.load_user('1') is None              # db failure -> logged out, no crash
    assert main.load_user('not-an-int') is None     # malformed cookie
    assert main.load_user(None) is None
    assert main.load_user('0') is None


def test_schema_migration_adds_missing_column_and_is_idempotent(monkeypatch):
    from sqlalchemy import inspect as sa_inspect
    with main.app.app_context():
        main.db.session.execute(main.db.text('CREATE TABLE IF NOT EXISTS _migration_probe (id INTEGER PRIMARY KEY)'))
        main.db.session.commit()
        monkeypatch.setattr(main, 'SCHEMA_COLUMNS', [('_migration_probe', 'added_col', 'VARCHAR(10)', 'VARCHAR(10)')])
        first = main.run_schema_migration()
        assert any('added_col' in entry for entry in first), first
        second = main.run_schema_migration()
        assert second == [], 'migration is not idempotent: %s' % second
        columns = {c['name'] for c in sa_inspect(main.db.engine).get_columns('_migration_probe')}
        assert 'added_col' in columns


def test_new_columns_exist_on_the_live_schema():
    from sqlalchemy import inspect as sa_inspect
    with main.app.app_context():
        campaign_columns = {c['name'] for c in sa_inspect(main.db.engine).get_columns('auto_video_post_campaign')}
        for column in ('source_type', 'source_ids', 'queue_state', 'posted_count',
                       'failed_count', 'skipped_count', 'rounds_completed',
                       'stop_requested', 'needs_reupload', 'vault_state'):
            assert column in campaign_columns, column
        user_columns = {c['name'] for c in sa_inspect(main.db.engine).get_columns('user')}
        assert 'is_banned' in user_columns and 'last_login_at' in user_columns


# ═══════════════════════════════════════════════════════════════════════════
# 24. Auto Video Poster — the campaign must keep sending after Railway wipes
#     the ephemeral disk ("[Errno 2] No such file or directory: /app/auto_post_media/…")
# ═══════════════════════════════════════════════════════════════════════════

def _zip_buffer(names):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as zf:
        for name in names:
            zf.writestr(name, name.encode() + b'-video-bytes')
    buffer.seek(0)
    return buffer


def _create_zip_campaign(client, account, channels, names=('one.mp4',)):
    return client.post('/bot/auto_video_post/create', data={
        'account_id': str(account), 'source_type': 'zip',
        'channels': '\n'.join(channels), 'caption': 'Video {name} - {index}/{total}',
        'min_delay': '5', 'max_delay': '15',
        'zip_files': (_zip_buffer(list(names)), 'videos.zip'),
    }, content_type='multipart/form-data')


def _media_dir(campaign_id):
    with main.app.app_context():
        return main.AutoVideoPostCampaign.query.get(campaign_id).media_dir


def _wipe_disk(campaign_id):
    """Simulate a redeploy: the extracted videos are gone, the DB row survives."""
    shutil.rmtree(_media_dir(campaign_id), ignore_errors=True)


def _payload_kinds(fake_tg):
    return [kind for _, kind in fake_tg.file_payloads]


def test_zip_round_uploads_each_video_once_and_remembers_it_on_telegram(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1', 'chan2', 'chan3']).get_json()
    campaign_id = payload['campaign_id']
    main._run_auto_post_round(campaign_id)
    state = _campaign_state(campaign_id)
    assert state['posted'] == 3, state
    assert state['status'] == 'running'
    # One real upload, then the other two channels re-use that Telegram copy.
    assert _payload_kinds(fake_tg) == ['upload', 'telegram-copy', 'telegram-copy'], fake_tg.file_payloads
    with main.app.app_context():
        vault = main._load_vault(main.AutoVideoPostCampaign.query.get(campaign_id))
    assert list(vault) == ['one.mp4'] and vault['one.mp4']['m'] > 0


def test_campaign_keeps_posting_after_the_disk_is_wiped(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1', 'chan2']).get_json()
    campaign_id = payload['campaign_id']
    main._run_auto_post_round(campaign_id)
    assert _campaign_state(campaign_id)['posted'] == 2

    _wipe_disk(campaign_id)
    fake_tg.reset()
    main._run_auto_post_round(campaign_id)          # must not raise and must not stall

    state = _campaign_state(campaign_id)
    assert state['posted'] == 4, state              # two more confirmed posts
    assert state['failed'] == 0 and 'No such file' not in (state['last_error'] or '')
    assert state['status'] == 'running'
    assert state['next_run_at'] is not None, 'a wiped disk must not stop the schedule'
    assert _payload_kinds(fake_tg) == ['telegram-copy', 'telegram-copy'], fake_tg.file_payloads


def test_vault_copy_is_reused_instead_of_reuploading_across_rounds(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']
    main._run_auto_post_round(campaign_id)
    assert _payload_kinds(fake_tg) == ['upload']
    fake_tg.reset()
    with main.app.app_context():                 # the file is still on disk here
        campaign = main.AutoVideoPostCampaign.query.get(campaign_id)
        campaign.queue_state = json.dumps({'version': 3, 'channels': {
            'chan1': {'order': [0], 'position': 0, 'cycle': 1}}})
        main.db.session.commit()
    main._run_auto_post_round(campaign_id)
    assert _payload_kinds(fake_tg) == ['telegram-copy'], fake_tg.file_payloads


def test_list_reports_vault_copies_and_clears_the_reupload_flag(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']
    main._run_auto_post_round(campaign_id)
    _wipe_disk(campaign_id)
    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(campaign_id)
        campaign.needs_reupload = True
        main.db.session.commit()
    card = [c for c in user_client.get('/bot/auto_video_post/list').get_json()['campaigns']
            if c['id'] == campaign_id][0]
    assert card['vault_count'] == 1
    assert card['needs_reupload'] is False
    assert card['available_videos'] == 1          # the Telegram copy keeps it usable


def test_restart_recovery_keeps_a_vault_backed_campaign_running(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']
    main._run_auto_post_round(campaign_id)
    _wipe_disk(campaign_id)
    main.recover_interrupted_tasks()
    assert _campaign_state(campaign_id)['status'] == 'running'


def test_restart_recovery_pauses_a_campaign_with_no_copy_at_all(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']
    _wipe_disk(campaign_id)                       # nothing posted yet -> no Telegram copy
    main.recover_interrupted_tasks()
    state = _campaign_state(campaign_id)
    assert state['status'] == 'paused', state
    assert state['last_error']


def test_add_videos_extends_a_running_campaign_and_keeps_counters(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1', 'chan2']).get_json()
    campaign_id = payload['campaign_id']
    main._run_auto_post_round(campaign_id)
    assert _campaign_state(campaign_id)['posted'] == 2

    r = user_client.post('/bot/auto_video_post/%d/add_videos' % campaign_id, data={
        'zip_files': (_zip_buffer(['two.mp4', 'three.mp4']), 'more.zip'),
    }, content_type='multipart/form-data')
    assert r.status_code == 200, r.data[:300]
    body = r.get_json()
    assert body['success'] is True and body['added'] == 2 and body['videos'] == 3

    after = _campaign_state(campaign_id)
    assert after['posted'] == 2, 'adding videos must never reset confirmed counters'
    assert after['status'] == 'running' and after['next_run_at'] is not None
    for channel, queue in after['queues']['channels'].items():
        assert sorted(queue['order']) == [0, 1, 2], (channel, queue)


def test_add_videos_revives_a_campaign_whose_media_was_wiped(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1']).get_json()
    campaign_id = payload['campaign_id']
    _wipe_disk(campaign_id)
    main.recover_interrupted_tasks()
    assert _campaign_state(campaign_id)['status'] == 'paused'

    r = user_client.post('/bot/auto_video_post/%d/add_videos' % campaign_id, data={
        'zip_files': (_zip_buffer(['fresh.mp4']), 'again.zip'),
    }, content_type='multipart/form-data')
    assert r.status_code == 200, r.data[:300]
    state = _campaign_state(campaign_id)
    assert state['status'] == 'running'
    assert state['last_error'] is None
    with main.app.app_context():
        assert main.AutoVideoPostCampaign.query.get(campaign_id).needs_reupload is False
    fake_tg.reset()
    main._run_auto_post_round(campaign_id)
    assert _campaign_state(campaign_id)['posted'] == 1, 'the revived campaign must post again'


def test_add_videos_rejects_a_zip_without_videos(user_client, account, fake_tg):
    payload = _create_zip_campaign(user_client, account, ['chan1']).get_json()
    r = user_client.post('/bot/auto_video_post/%d/add_videos' % payload['campaign_id'], data={
        'zip_files': (_zip_buffer([]), 'empty.zip'),
    }, content_type='multipart/form-data')
    assert r.status_code == 400
    assert 'video' in r.get_json()['error'].lower()


def test_ban_flag_actually_persists(user_client, admin_client, fake_tg):
    with main.app.app_context():
        victim = main.User(username='victim', password=main.generate_password_hash('pw123456'))
        main.db.session.add(victim)
        main.db.session.commit()
        victim_id = victim.id
    r = admin_client.post('/bot/admin/ban_user', json={'user_id': victim_id, 'ban': True})
    assert r.status_code in (200, 201), r.data[:200]
    with main.app.app_context():
        assert main.User.query.get(victim_id).is_banned is True
    users = admin_client.get('/bot/admin/users').get_json()['users']
    row = [u for u in users if u['id'] == victim_id][0]
    assert row['is_banned'] is True


def test_saved_targets_roundtrip(user_client, fake_tg):
    r = user_client.post('/bot/saved_targets', json={
        'kind': 'channel', 'identifiers': ['@alpha', 't.me/beta', 'bad name!!'],
    })
    assert r.status_code == 200
    body = r.get_json()
    assert body['added'] == 2 and body['invalid'] == ['bad name!!']
    listed = user_client.get('/bot/saved_targets').get_json()['targets']
    assert sorted(t['identifier'] for t in listed) >= ['alpha', 'beta']
    # saving the same list again must not create duplicates
    assert user_client.post('/bot/saved_targets', json={
        'kind': 'channel', 'identifiers': ['@alpha'],
    }).get_json()['added'] == 0


def test_session_export_import_round_trip(user_client, account):
    """Proves the operator's workflow: download the sessions to a file today,
    lose the database (a brand-new Railway service), import the file later and
    get the accounts back."""
    export = user_client.get('/bot/export_accounts')
    assert export.status_code == 200
    assert 'attachment; filename="tgbot_backup_' in export.headers.get('Content-Disposition', '')
    payload = json.loads(export.data.decode('utf-8'))
    assert payload['version'] == 1
    assert payload['exported_at'] and payload['warning']
    phones = [a['phone'] for a in payload['accounts']]
    assert '+10000000001' in phones
    assert all(a['session_string'] for a in payload['accounts'])
    assert all(a['session_string'] not in ('demo', 'active') for a in payload['accounts'])

    # simulate an empty database on a new service
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        for row in main.TelegramAccount.query.filter_by(user_id=user.id, phone='+10000000001').all():
            main.db.session.delete(row)
        main.db.session.commit()
        assert main.TelegramAccount.query.filter_by(user_id=user.id, phone='+10000000001').first() is None

    restore = user_client.post('/bot/import_accounts', json=payload)
    assert restore.status_code == 200, restore.data[:300]
    body = restore.get_json()
    assert body['success'] is True and body['imported'] >= 1, body
    with main.app.app_context():
        restored = main.TelegramAccount.query.filter_by(phone='+10000000001').first()
        assert restored is not None
        assert restored.session_string == 'FAKE-SESSION-STRING-FOR-TESTS'
        assert restored.is_active is True

    # importing the same backup twice must not duplicate anything
    again = user_client.post('/bot/import_accounts', json=payload).get_json()
    assert again['imported'] == 0 and again['skipped'] >= 1, again


def test_import_rejects_an_invalid_backup_file(user_client):
    r = user_client.post('/bot/import_accounts', json={'nonsense': True})
    assert r.status_code == 200
    assert r.get_json()['success'] is False


# ═══════════════════════════════════════════════════════════════════════════
# 11. Generic scheduler + group-link messaging (every messaging tool)
# ═══════════════════════════════════════════════════════════════════════════

def _sched_payload(account_id, **kw):
    payload = {'kind': 'broadcast', 'account_ids': [account_id],
               'destinations': '@chan1\n@chan2', 'message': 'hello', 'delay': 1, 'send_now': True}
    payload.update(kw)
    return payload


def _sched_row(client, task_id):
    for task in client.get('/bot/scheduler/list').get_json()['tasks']:
        if task['id'] == task_id:
            return task
    return None


def test_scheduler_validation_for_every_kind(user_client, account, fake_tg):
    assert user_client.post('/bot/scheduler/create', json={
        'kind': 'broadcast', 'destinations': '@c', 'message': 'x'}).status_code == 400        # no account
    r = user_client.post('/bot/scheduler/create', json=_sched_payload(account, destinations=''))
    assert r.status_code == 400 and 'destination' in r.get_json()['error'].lower()
    assert user_client.post('/bot/scheduler/create', json=_sched_payload(account, kind='nope')).status_code == 400
    assert user_client.post('/bot/scheduler/create', json=_sched_payload(account, message='', media_path='')).status_code == 400
    for kind in ('dm', 'dm_campaign', 'group_links', 'personal', 'channel_post'):
        ok = user_client.post('/bot/scheduler/create', json=_sched_payload(account, kind=kind))
        assert ok.status_code == 200, (kind, ok.data[:200])


def test_scheduler_rejects_bad_times_and_repeat_settings(user_client, account, fake_tg):
    past = _sched_payload(account, send_now=False, scheduled_at='2000-01-01T00:00')
    assert user_client.post('/bot/scheduler/create', json=past).status_code == 400
    junk = _sched_payload(account, send_now=False, scheduled_at='totally-not-a-date')
    assert user_client.post('/bot/scheduler/create', json=junk).status_code == 400
    no_interval = _sched_payload(account, repeat_count=3, repeat_interval_minutes=0)
    assert user_client.post('/bot/scheduler/create', json=no_interval).status_code == 400


def test_scheduler_full_lifecycle(user_client, account, fake_tg):
    future = (main.datetime.utcnow() + main.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
    created = user_client.post('/bot/scheduler/create', json=_sched_payload(
        account, send_now=False, scheduled_at=future, title='Lifecycle task'))
    assert created.status_code == 200, created.data[:300]
    task_id = created.get_json()['task']['id']
    assert _sched_row(user_client, task_id)['status'] == 'pending'

    assert user_client.post('/bot/scheduler/%d/pause' % task_id).status_code == 200
    assert _sched_row(user_client, task_id)['status'] == 'paused'
    assert user_client.post('/bot/scheduler/%d/resume' % task_id).status_code == 200
    assert _sched_row(user_client, task_id)['status'] == 'pending'

    edited = user_client.patch('/bot/scheduler/%d' % task_id,
                               json={'message': 'edited text', 'destinations': '@onlyone'})
    assert edited.status_code == 200
    assert edited.get_json()['task']['destination_count'] == 1
    assert edited.get_json()['task']['message'] == 'edited text'

    assert user_client.post('/bot/scheduler/%d/cancel' % task_id).status_code == 200
    assert _sched_row(user_client, task_id)['status'] == 'cancelled'
    assert user_client.delete('/bot/scheduler/%d' % task_id).status_code == 200
    assert _sched_row(user_client, task_id) is None


def test_scheduled_task_runs_exactly_once_with_honest_counts(user_client, account, fake_tg):
    created = user_client.post('/bot/scheduler/create', json=_sched_payload(account))
    task_id = created.get_json()['task']['id']
    assert main._scheduler_run_task(task_id) is True
    row = _sched_row(user_client, task_id)
    assert row['status'] == 'completed' and row['sent'] == 2 and row['failed'] == 0
    assert len(fake_tg.sent) == 2
    # claim guard: a second attempt must not send anything again
    before = len(fake_tg.sent)
    assert main._scheduler_run_task(task_id) is False
    assert len(fake_tg.sent) == before


def test_scheduled_task_partial_failure_is_not_reported_as_success(user_client, account, fake_tg):
    fake_tg.fail_entity_for = {'chan2'}
    created = user_client.post('/bot/scheduler/create', json=_sched_payload(account))
    task_id = created.get_json()['task']['id']
    main._scheduler_run_task(task_id)
    row = _sched_row(user_client, task_id)
    assert row['sent'] == 1 and row['failed'] == 1 and row['status'] == 'partial'
    assert row['errors'], 'per-destination failure reasons must be reported'


def test_scheduled_task_repeat_rearms_without_duplicating(user_client, account, fake_tg):
    created = user_client.post('/bot/scheduler/create', json=_sched_payload(
        account, repeat_interval_minutes=60, repeat_count=2))
    task_id = created.get_json()['task']['id']
    main._scheduler_run_task(task_id)
    row = _sched_row(user_client, task_id)
    assert row['status'] == 'pending' and row['runs_done'] == 1 and row['repeat_count'] == 1
    assert row['next_run_at']
    assert row['sent'] == 2


def test_multi_account_schedule_uses_round_robin(user_client, account, fake_tg):
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        second = main.TelegramAccount(user_id=user.id, phone='+10000000002',
                                      session_string='FAKE-SESSION-2', is_active=True)
        main.db.session.add(second)
        main.db.session.commit()
        second_id = second.id
    created = user_client.post('/bot/scheduler/create', json=_sched_payload(
        account, account_ids=[account, second_id], destinations='@dest1\n@dest2\n@dest3\n@dest4'))
    assert created.status_code == 200, created.data[:300]
    body = created.get_json()
    assert body['execution_strategy']
    task_id = body['task']['id']
    main._scheduler_run_task(task_id)
    row = _sched_row(user_client, task_id)
    assert row['sent'] == 4 and row['total'] == 8      # 4 destinations × 2 accounts


def test_group_link_resolution_reports_unusable_destinations(user_client, account, fake_tg):
    fake_tg.fail_entity_for = {'brokenchan'}
    r = user_client.post('/bot/group_links/resolve', json={
        'account_id': account, 'links': 't.me/goodchan\nbrokenchan\nnot a link!!'})
    assert r.status_code == 200
    body = r.get_json()
    assert body['success'] is True
    assert body['resolved'] == 1 and body['unresolved'] == 1
    assert body['invalid'] == ['not a link!!']
    by_dest = {item['destination']: item for item in body['results']}
    assert by_dest['t.me/goodchan']['ok'] is True
    assert by_dest['brokenchan']['ok'] is False and by_dest['brokenchan']['reason']
    assert body['note']


def test_normalize_destination_list_dedupes_and_flags_invalid():
    valid, invalid = main.normalize_destination_list('@alpha\nhttps://t.me/beta\nt.me/alpha\nbad name!!')
    # the operator's original text is preserved; the duplicate @alpha form is dropped
    assert valid == ['@alpha', 'https://t.me/beta']
    assert invalid == ['bad name!!']
    # Telegram usernames are at least 4-5 characters, so short ones are unusable
    short_valid, short_invalid = main.normalize_destination_list('@a')
    assert short_valid == [] and short_invalid == ['@a']


def test_legacy_sqlite_import_is_disabled_by_default():
    result = main.import_legacy_sqlite()
    assert result['skipped'] is True


def test_upload_limit_returns_json_413():
    old_testing = main.app.config['TESTING']
    old_limit = main.app.config['MAX_CONTENT_LENGTH']
    main.app.config['TESTING'] = False
    main.app.config['PROPAGATE_EXCEPTIONS'] = False
    main.app.config['MAX_CONTENT_LENGTH'] = 2048
    try:
        c = main.app.test_client()
        c.get('/logout')
        c.post('/login', data={'username': TEST_USER, 'password': TEST_USER_PASS})
        r = c.post('/bot/auto_video_post/create', data={
            'account_id': '1', 'channels': 'chan1',
            'zip_files': (io.BytesIO(b'x' * 4096), 'big.zip'),
        }, content_type='multipart/form-data')
        assert r.status_code == 413
        assert r.is_json and 'limit' in r.get_json()['error'].lower()
    finally:
        main.app.config['TESTING'] = old_testing
        main.app.config['MAX_CONTENT_LENGTH'] = old_limit


def test_error_responses_never_leak_secrets(user_client, fake_tg):
    with main.app.app_context():
        user = main.User.query.filter_by(username=TEST_USER).first()
        account_id = (main.TelegramAccount.query.filter_by(user_id=user.id).first()).id
    fake_tg.fail_entity_for = {'chan1'}
    r = user_client.post('/bot/dm_campaign', json={
        'account_id': account_id, 'targets': [{'username': 'x'}], 'messages': ['hi'],
    })
    body = r.data.decode('utf-8', 'ignore')
    assert 'FAKE-SESSION-STRING-FOR-TESTS' not in body
    assert 'abcdef' not in body      # api_hash


# ═══════════════════════════════════════════════════════════════════════════
# 9. Deployment self-diagnostics (the "Internal server error" on /login)
# ═══════════════════════════════════════════════════════════════════════════

def test_diagnostics_endpoint_reports_readiness(app_client):
    r = app_client.get('/diag')
    assert r.status_code == 200
    payload = r.get_json()
    assert payload['templates_found']['login.html'] is True
    assert payload['templates_found']['dashboard.html'] is True
    assert payload['database']['connected'] is True
    assert 'environment' in payload and 'limits' in payload
    dump = json.dumps(payload).lower()
    for secret in ('password=', 'api_hash', 'session_string', 'npg_'):
        assert secret not in dump


def test_login_still_renders_when_the_template_is_missing(monkeypatch, app_client):
    """A deployment without templates/ must still serve a usable login form."""
    monkeypatch.setattr(main, 'render_template',
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError('template missing')))
    r = app_client.get('/login')
    assert r.status_code == 200
    assert b'name="password"' in r.data
    assert b'action="/login"' in r.data


def test_login_reports_database_failure_clearly(monkeypatch, app_client):
    """A TRULY unreachable database gives 503 with an actionable message."""
    class BrokenQuery:
        @staticmethod
        def filter_by(**kwargs):
            raise RuntimeError('database down')

    class BrokenUser:
        query = BrokenQuery()

    def broken_execute(*args, **kwargs):
        raise RuntimeError('database down')

    monkeypatch.setattr(main, 'User', BrokenUser)
    monkeypatch.setattr(main.db.session, 'execute', broken_execute)
    monkeypatch.setattr(main.db.session, 'rollback', lambda *a, **k: None)
    r = app_client.post('/login', data={'username': 'x', 'password': 'y'})
    assert r.status_code == 503
    assert b'database' in r.data.lower()


def test_migration_ddl_quotes_postgres_reserved_words():
    """Regression: `ALTER TABLE user ...` is a PostgreSQL syntax error, which is
    why is_banned/last_login_at were never created and login failed with
    'column user.is_banned does not exist'."""
    ddl = main._migration_ddl('user', 'is_banned', 'BOOLEAN DEFAULT FALSE', True)
    assert ddl == 'ALTER TABLE "user" ADD COLUMN "is_banned" BOOLEAN DEFAULT FALSE'
    sqlite_ddl = main._migration_ddl('user', 'is_banned', 'BOOLEAN DEFAULT 0', False)
    assert sqlite_ddl == 'ALTER TABLE user ADD COLUMN is_banned BOOLEAN DEFAULT 0'


def test_login_survives_a_missing_column_on_the_database(monkeypatch, app_client):
    """The production bug: the ORM query raises because is_banned does not exist.
    The raw-SQL fallback must still let the administrator in."""
    class BrokenQuery:
        @staticmethod
        def filter_by(**kwargs):
            raise RuntimeError('column user.is_banned does not exist')

        @staticmethod
        def get(_ident):
            raise RuntimeError('column user.is_banned does not exist')

    class StubUser:
        query = BrokenQuery()

        def __init__(self, username='', password=''):
            self.username = username
            self.password = password
            self.id = None
            self.is_banned = False
            self.last_login_at = None

        def get_id(self):
            return str(self.id)

        is_authenticated = property(lambda self: True)
        is_active = property(lambda self: True)
        is_anonymous = property(lambda self: False)

    monkeypatch.setattr(main, 'User', StubUser)
    app_client.get('/logout')
    r = app_client.post('/login', data={'username': TEST_USER, 'password': TEST_USER_PASS})
    assert r.status_code in (302, 303), r.data[:300]
    # the session must keep working on the next request too (load_user fallback)
    assert app_client.get('/bot/dashboard_stats').status_code == 200


def test_diag_reports_missing_columns_and_ok_status(app_client):
    payload = app_client.get('/diag').get_json()
    assert payload['database']['missing_columns'] == []
    assert payload['status'] == 'ok'
    assert payload['environment']['SECRET_KEY_is_placeholder'] is False


# ═══════════════════════════════════════════════════════════════════════════
# Caption pool — one random caption per round, identical for every channel
# ═══════════════════════════════════════════════════════════════════════════

def test_parse_caption_pool_accepts_the_json_array_the_dashboard_sends():
    """The caption boxes are submitted as a JSON array (one entry per box)."""
    assert main._parse_caption_pool('["one {index}", "two\\nlines"]') == ['one {index}', 'two\nlines']


def test_parse_caption_pool_splits_a_dash_line():
    assert main._parse_caption_pool('a\n\nb\n---\nc') == ['a\n\nb', 'c']
    assert main._parse_caption_pool('a\n-----\nb') == ['a', 'b']


def test_parse_caption_pool_keeps_multiline_captions():
    # a caption may legitimately contain blank lines
    assert main._parse_caption_pool('line1\n\nline2') == ['line1\n\nline2']


def test_parse_caption_pool_two_dashes_are_not_a_separator():
    assert main._parse_caption_pool('x\n--\ny') == ['x\n--\ny']


def test_parse_caption_pool_empty_input():
    assert main._parse_caption_pool('') == []
    assert main._parse_caption_pool(None) == []
    assert main._parse_caption_pool('   \n\t ') == []
    assert main._parse_caption_pool('---') == []


def test_campaign_captions_falls_back_to_the_single_caption():
    """Existing campaigns (no pool) must behave exactly as before."""
    class C(object):
        pass
    c = C()
    c.captions = None
    c.caption = 'single {name}'
    assert main._campaign_captions(c) == ['single {name}']

    c.captions = '["a {index}", "b {index}"]'
    assert main._campaign_captions(c) == ['a {index}', 'b {index}']

    c.captions = '[]'
    c.caption = ''
    assert main._campaign_captions(c) == ['']


def test_caption_placeholders_still_render_from_a_pool_entry():
    out = main._auto_caption('hi {name} {index}/{total} {channel}', 'clip_2.mp4', 1, 5, '@chan')
    assert out == 'hi clip_2 2/5 @chan'


# ═══════════════════════════════════════════════════════════════════════════
# Wiped media folder (Railway /data is ephemeral) + interval must obey the operator
# ═══════════════════════════════════════════════════════════════════════════

def test_round_recreates_a_wiped_media_folder(user_client, account, fake_tg):
    """Reproduces the reported production failure.

    A redeploy wipes the campaign folder while the campaign row survives in
    PostgreSQL. The next round then died with

        [Errno 2] No such file or directory: '/data/auto_post_media/<uid>'

    which failed the whole round (0 posted). The round must recreate the folder.
    """
    import os as _os
    import shutil as _shutil

    payload = _create_saved_campaign(user_client, account, ['chan1']).get_json()
    cid = payload['campaign_id']

    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(cid)
        media_dir = campaign.media_dir
    assert media_dir, 'the campaign should have a media folder'

    # simulate the redeploy
    _shutil.rmtree(media_dir, ignore_errors=True)
    assert not _os.path.exists(media_dir), 'setup failed: the folder still exists'

    main._run_auto_post_round(cid)

    assert _os.path.isdir(media_dir), 'the round did not recreate the wiped media folder'
    state = _campaign_state(cid)
    assert state['posted'] == 1, state
    assert 'No such file or directory' not in (state.get('last_error') or ''), state


def test_empty_round_waits_the_operators_own_delay(user_client, account, fake_tg, monkeypatch):
    """A round that posts nothing must not silently wait longer than the setting.

    The old code doubled the delay and floored it at 300-900s, so a campaign set to
    3 minutes waited 6-15 minutes instead. With min = max = 3 the wait must be ~180s.
    """
    payload = _create_saved_campaign(user_client, account, ['chan1'], min_delay=3, max_delay=3).get_json()
    cid = payload['campaign_id']

    # make the one channel fail, so counters['posted'] == 0
    def boom(*_args, **_kwargs):
        raise RuntimeError('simulated send failure')
    monkeypatch.setattr(main, '_auto_caption', boom)

    before = main.datetime.utcnow()
    main._run_auto_post_round(cid)
    after = main.datetime.utcnow()

    with main.app.app_context():
        campaign = main.AutoVideoPostCampaign.query.get(cid)
        nxt = campaign.next_run_at

    state = _campaign_state(cid)
    assert state['posted'] == 0, state
    assert nxt is not None, 'the campaign was not re-armed'
    delta = (nxt - before).total_seconds()
    # ~180s measured from the round's START. The old code produced >= 360s here.
    assert 150 <= delta <= 200, 'waited %ss instead of the operator 180s' % delta


def test_a_folder_as_the_media_path_is_skipped_not_sent(user_client, account, fake_tg, monkeypatch):
    """The reported '[Errno 2] ... /data/auto_post_media/<uid>' must never fail a round.

    That path is a campaign media FOLDER. If such a path reaches Telegram as the file to
    upload, the channel failed and the whole round was written off. It must now be a
    clean per-channel skip so the other channels still post.
    """
    import os as _os
    import tempfile

    folder = tempfile.mkdtemp()
    try:
        payload = _create_saved_campaign(user_client, account, ['chan1'], min_delay=1, max_delay=1).get_json()
        cid = payload['campaign_id']
        # the "video" is an existing folder -> passed the old os.path.exists() check
        monkeypatch.setattr(main, '_campaign_videos', lambda c: ([folder], 'zip', None))
        monkeypatch.setattr(main, '_load_vault', lambda c: {})

        main._run_auto_post_round(cid)

        state = _campaign_state(cid)
        assert state['posted'] == 0, state
        assert state['failed'] == 0, (
            'a bad path must not count as a send failure: %s' % state)
        assert state['skipped'] == 1, state
        err = state.get('last_error') or ''
        assert 'No such file or directory' not in err, err
        assert 'Is a directory' not in err, err
    finally:
        _os.rmdir(folder)


def test_zip_media_that_is_a_missing_file_is_skipped_cleanly(user_client, account, fake_tg, monkeypatch):
    """Same guarantee for a plain missing file path (wiped disk, no Telegram copy)."""
    payload = _create_saved_campaign(user_client, account, ['chan1'], min_delay=1, max_delay=1).get_json()
    cid = payload['campaign_id']
    monkeypatch.setattr(main, '_campaign_videos',
                        lambda c: (['/nonexistent/auto_post_media/gone.mp4'], 'zip', None))
    monkeypatch.setattr(main, '_load_vault', lambda c: {})

    main._run_auto_post_round(cid)

    state = _campaign_state(cid)
    assert state['posted'] == 0, state
    assert state['skipped'] == 1, state
    assert 'No such file or directory' not in (state.get('last_error') or ''), state


def test_stop_endpoint_marks_a_run_stopped(user_client, account):
    """A running campaign must be stoppable — it previously was not.

    With a long delay the operator could only watch 'Sent: 1/64' for hours; there was
    no stop/cancel endpoint for broadcast at all.
    """
    key = 'broadcast:stop-test-1'
    assert main.task_should_stop(key) is False

    res = user_client.post('/bot/task/%s/stop' % key)
    assert res.status_code == 200, res.get_data(as_text=True)
    assert res.get_json()['success'] is True
    assert main.task_should_stop(key) is True, 'the worker would never see the stop'

    # The worker finishing afterwards must report 'stopped', not 'completed'.
    main.task_run_finish(key, 'completed')
    assert main.task_should_stop(key) is False, 'the flag must be cleared once consumed'


def test_stop_flag_is_cleared_so_later_runs_are_unaffected():
    key = 'broadcast:stop-test-2'
    main.task_request_stop(key)
    assert main.task_should_stop(key) is True
    main.task_clear_stop(key)
    assert main.task_should_stop(key) is False


# ═══════════════════════════════════════════════════════════════════════════
# Repeating broadcast rounds must be DURABLE, not held in one thread
# ═══════════════════════════════════════════════════════════════════════════
# Report: "4 hour diye start now korsi, start hoyese but 4 hr por por se same babe
# kore na". Start Now ran its whole round loop inside the request thread — a full
# round, then asyncio.sleep(delay), then the next round. That only works while the
# one process stays alive. On Railway a redeploy/crash/platform restart during the
# multi-hour wait killed the thread, the campaign stayed 'running' forever and
# round 2 never happened. The delay was never the problem.
#
# Rounds are now bookkept in the database (rounds_done / next_round_at) and driven
# by `run_broadcast_scheduler`, which starts with the other boot jobs. These tests
# pin that down: the durable booking, the stop/delete cleanup that stops the
# scheduler re-booking a campaign the operator has killed, and the restart resume.


def _broadcast_row(task_id):
    with main.app.app_context():
        return main.BroadcastTask.query.get(task_id)


def _start_repeating(client, account_id, groups, repeat, delay=1):
    return client.post('/bot/broadcast', json={
        'account_id': account_id, 'content': 'hi', 'group_ids': groups,
        'delay': delay, 'repeat': repeat,
    })


def _card_for(client, task_id):
    data = client.get('/bot/broadcast/campaigns').get_json()
    key = 'broadcast:%d' % task_id
    return [c for c in data['campaigns'] if c['task_key'] == key]


@pytest.mark.parametrize('key,expected', [
    ('broadcast:12', 12),
    ('broadcast:0', 0),
    ('broadcast:', None),
    ('broadcast:abc', None),
    ('broadcast', None),
    ('', None),
    (None, None),
    ('dm:12', None),
])
def test_broadcast_task_id_from_key(key, expected):
    assert main.broadcast_task_id_from_key(key) == expected


def test_broadcast_round_columns_exist_and_are_migrated():
    """A new column with no SCHEMA_COLUMNS entry never appears on Railway."""
    cols = {'caption', 'repeat', 'rounds_done', 'next_round_at', 'task_key'}
    for name in cols:
        assert hasattr(main.BroadcastTask, name), 'model is missing %s' % name
    migrated = {c[1] for c in main.SCHEMA_COLUMNS if c[0] == 'broadcast_task'}
    assert cols <= migrated, 'unmigrated column(s): %s' % sorted(cols - migrated)


def test_every_schema_column_entry_is_one_of_the_four_documented_fields():
    # Cheap guard against a malformed row silently breaking the migration loop.
    for entry in main.SCHEMA_COLUMNS:
        assert len(entry) == 4, entry
        table, column, sqlite_type, pg_type = entry
        assert table and column and sqlite_type and pg_type, entry


def test_broadcast_scheduler_is_started_at_boot():
    src = (PROJECT_ROOT / 'main.py').read_text(encoding='utf-8')
    assert "start_job_once('broadcast_scheduler', run_broadcast_scheduler)" in src, \
        'the durable scheduler is never started, so rounds 2..N would never run'


def test_the_in_memory_round_loop_is_gone():
    src = (PROJECT_ROOT / 'main.py').read_text(encoding='utf-8')
    assert 'def run_broadcast(' not in src, 'the thread-only round loop is back'
    assert 'while ((repeat == 0 or round_no < repeat)' not in src


def test_repeating_broadcast_books_the_next_round_in_the_database(user_client, account, fake_tg):
    r = _start_repeating(user_client, account, ['grp1', 'grp2'], repeat=3)
    assert r.status_code == 200, r.data[:300]
    task_id = r.get_json()['task_id']

    def booked():
        row = _broadcast_row(task_id)
        return row is not None and row.rounds_done == 1 and row.next_round_at is not None

    assert _wait_for(booked), 'round 1 must book round 2 in the database'
    row = _broadcast_row(task_id)
    assert row.status == 'running', 'a 3-round campaign is not finished after round 1'
    assert row.rounds_done == 1
    assert row.sent_count == 2, 'sent_count accumulates across rounds'
    assert row.next_round_at > main.datetime.utcnow(), 'next round must be in the future'
    assert row.task_key == 'broadcast:%d' % task_id


def test_the_scheduler_running_a_booked_round_continues_the_campaign(user_client, account, fake_tg):
    """After the wait, the scheduler calls exactly this function for round 2."""
    r = _start_repeating(user_client, account, ['grp1'], repeat=2)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)

    # Pretend the interval elapsed and the scheduler fired this campaign.
    with main.app.app_context():
        row = main.BroadcastTask.query.get(task_id)
        row.next_round_at = main.datetime.utcnow() - main.timedelta(seconds=1)
        main.db.session.commit()

    main.broadcast_run_one_round(task_id)

    row = _broadcast_row(task_id)
    assert row.status == 'completed', row.status
    assert row.rounds_done == 2
    assert row.sent_count == 2
    assert len([s for s in fake_tg.sent if s[0] == 'grp1']) == 2, 'round 2 did not send'


def test_infinite_rounds_keep_getting_booked(user_client, account, fake_tg):
    """repeat=0 is the UI's "keep repeating every delay until Stop"."""
    r = _start_repeating(user_client, account, ['grp1'], repeat=0)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)
    row = _broadcast_row(task_id)
    assert row.status == 'running', 'repeat=0 must never finish on its own'
    assert row.next_round_at is not None


def test_a_campaign_that_never_booked_is_still_picked_up_after_a_restart(user_client, account, fake_tg):
    """A process that died mid-round leaves next_round_at NULL — that must resume."""
    r = _start_repeating(user_client, account, ['grp1'], repeat=2)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)

    with main.app.app_context():          # simulate a crash before booking
        row = main.BroadcastTask.query.get(task_id)
        row.next_round_at = None
        main.db.session.commit()

    # The scheduler treats "running with no booking" as due immediately.
    now = main.datetime.utcnow()
    with main.app.app_context():
        due = [x.id for x in main.BroadcastTask.query
               .filter(main.BroadcastTask.status == 'running').all()
               if x.next_round_at is None or x.next_round_at <= now]
    assert task_id in due, 'an unbooked running campaign would be stranded'


def test_stopping_a_repeating_broadcast_clears_the_schedule(user_client, account, fake_tg):
    """Otherwise the scheduler re-books the round the operator just stopped."""
    r = _start_repeating(user_client, account, ['grp1'], repeat=0)
    task_id = r.get_json()['task_id']
    key = r.get_json()['task_key']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)

    res = user_client.post('/bot/task/%s/stop' % key)
    assert res.status_code == 200, res.get_data(as_text=True)
    row = _broadcast_row(task_id)
    assert row.status == 'stopped', row.status
    assert row.next_round_at is None, 'a stopped campaign must not stay due'


def test_a_second_campaign_on_the_same_account_is_refused_durably(user_client, account, fake_tg):
    """The account lock is free while a campaign waits between rounds."""
    first = _start_repeating(user_client, account, ['grp1'], repeat=0)
    assert first.status_code == 200, first.data[:300]

    second = _start_repeating(user_client, account, ['grp2'], repeat=1)
    assert second.status_code == 409, second.data[:300]
    assert 'already' in second.get_json()['error'].lower()


def test_campaign_list_exposes_round_progress(user_client, account, fake_tg):
    r = _start_repeating(user_client, account, ['grp1', 'grp2'], repeat=4)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)

    cards = _card_for(user_client, task_id)
    assert len(cards) == 1, cards
    card = cards[0]
    assert card['rounds_done'] == 1
    assert card['rounds_total'] == 4
    assert card['next_round_in'] is not None and card['next_round_in'] > 0, card
    assert card['lifetime_sent'] == 2
    assert card['targets'] == 2


def test_deleting_a_campaign_removes_the_durable_row(user_client, account, fake_tg):
    r = _start_repeating(user_client, account, ['grp1'], repeat=0)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)
    card = _card_for(user_client, task_id)[0]

    res = user_client.delete('/bot/broadcast/campaigns/%d' % card['id'])
    assert res.status_code == 200, res.get_data(as_text=True)
    assert _broadcast_row(task_id) is None, 'a deleted campaign must not stay queued'


def test_campaign_card_renders_round_progress_and_a_countdown():
    html = _read_template('dashboard.html')
    assert '_bcCountdown' in html, 'no countdown helper'
    assert 'next round in' in html, 'a waiting campaign looks finished without this'
    assert 'rounds_done' in html and 'lifetime_sent' in html


def test_a_campaign_always_gets_its_task_key_in_the_same_commit(user_client, account, fake_tg):
    """A running row with a NULL task_key would be swept as a stranded campaign."""
    r = _start_repeating(user_client, account, ['grp1'], repeat=1)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).status != 'running')
    assert _broadcast_row(task_id).task_key == 'broadcast:%d' % task_id


def test_stranded_pre_upgrade_campaigns_are_closed_not_resumed(user_client, account, fake_tg):
    """Rows stranded 'running' by the removed thread loop must not fire a round.

    Upgrading must never send: a campaign the old code left behind has no round
    bookkeeping, so the scheduler cannot tell which round it reached. Closing it
    is the only safe choice — the operator restarts it deliberately.
    """
    with main.app.app_context():
        user_id = main.User.query.first().id
        stranded = main.BroadcastTask(
            user_id=user_id, account_id=account, message_type='text', content='old',
            groups=json.dumps(['grp1', 'grp2']), delay=14400, status='running',
        )
        main.db.session.add(stranded)
        main.db.session.commit()
        stranded_id = stranded.id

    assert main.close_stranded_legacy_campaigns() >= 1
    row = _broadcast_row(stranded_id)
    assert row.status == 'stopped', row.status
    assert row.next_round_at is None
    assert not [s for s in fake_tg.sent if s[0] in ('grp1', 'grp2')], \
        'a stranded campaign must not send anything on upgrade'


def test_the_boot_sweep_does_not_touch_a_live_campaign(user_client, account, fake_tg):
    r = _start_repeating(user_client, account, ['grp1'], repeat=0)
    task_id = r.get_json()['task_id']
    assert _wait_for(lambda: (_broadcast_row(task_id) or 0) and
                     _broadcast_row(task_id).rounds_done == 1)

    main.close_stranded_legacy_campaigns()

    row = _broadcast_row(task_id)
    assert row.status == 'running', 'the sweep killed a legitimate campaign'
    assert row.next_round_at is not None
