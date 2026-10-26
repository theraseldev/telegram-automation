#!/usr/bin/env python3
"""Restore a MR ADS SQL backup into any PostgreSQL database.

Usage
-----
    # target comes from DATABASE_URL (Railway / Neon / Render / local)
    python tools/restore_backup.py neon_schema.sql

    # or pass the target explicitly
    python tools/restore_backup.py neon_schema.sql --url "postgresql://user:pass@host/db"

The script is written for the dumps produced by this project's backup routine:
``BEGIN; CREATE TABLE IF NOT EXISTS ...; INSERT ...; setval(...); COMMIT;``

It is safe by default: it refuses to touch a database that already holds real data
unless ``--force`` is given, and every statement runs on its own so one legacy
statement cannot abort the whole restore.
"""
import argparse
import io
import os
import re
import sys

import psycopg2

APP_TABLES_WITH_DATA = ('telegram_account', 'user', 'activity_log', 'scheduled_task')


def normalize(url):
    url = (url or '').strip().strip('"').strip("'")
    if url.startswith('postgres://'):
        url = 'postgresql://' + url[len('postgres://'):]
    if 'channel_binding=' in url:
        base, _, query = url.partition('?')
        kept = [p for p in query.split('&') if p and not p.startswith('channel_binding=')]
        url = base + (('?' + '&'.join(kept)) if kept else '')
    if 'sslmode=' not in url:
        url += ('&' if '?' in url else '?') + 'sslmode=require'
    return url


def split_statements(sql):
    """Split on ';' but respect single-quoted strings and ``--`` comments.

    Captions in a real backup contain newlines, so splitting per line cuts a row in
    half and Postgres then reports "unterminated quoted string". A character scanner
    keeps those rows whole.
    """
    statements, buf, i, n = [], [], 0, len(sql)
    in_str = False
    while i < n:
        ch = sql[i]
        if not in_str and sql.startswith('--', i):
            j = sql.find('\n', i)
            i = n if j < 0 else j
            continue
        if ch == "'":
            if in_str and i + 1 < n and sql[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_str = not in_str
            buf.append(ch)
        elif ch == ';' and not in_str:
            statements.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = ''.join(buf).strip()
    if tail:
        statements.append(tail)
    return [s for s in statements if s]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sql_file')
    parser.add_argument('--url', default=os.environ.get('DATABASE_URL', ''))
    parser.add_argument('--force', action='store_true',
                        help='truncate existing data before restoring')
    args = parser.parse_args()

    if not args.url:
        sys.exit('No target database. Set DATABASE_URL or pass --url.')
    sql = io.open(args.sql_file, encoding='utf-8-sig').read()

    conn = psycopg2.connect(normalize(args.url))
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("select table_name from information_schema.tables where table_schema='public'")
    existing = {r[0] for r in cur.fetchall()}

    has_data = False
    for table in APP_TABLES_WITH_DATA:
        if table in existing:
            cur.execute('select count(*) from "%s"' % table)
            if cur.fetchone()[0]:
                has_data = True
                break

    if has_data and not args.force:
        sys.exit('Target already contains data (refusing to merge). Re-run with --force '
                 'to replace it, or point --url at an empty database.')

    if existing:
        cur.execute('TRUNCATE TABLE ' + ', '.join('"%s"' % t for t in existing)
                    + ' RESTART IDENTITY CASCADE')
        print('truncated %d existing tables' % len(existing))

    for seq in sorted(set(re.findall(r"nextval\('([A-Za-z0-9_]+)'::regclass\)", sql))):
        cur.execute('CREATE SEQUENCE IF NOT EXISTS "%s"' % seq)

    executed = failed = 0
    added_columns = []
    for stmt in split_statements(sql):
        if stmt.strip() in ('BEGIN;', 'COMMIT;'):
            continue
        try:
            cur.execute(stmt)
            executed += 1
        except psycopg2.errors.UndefinedColumn as exc:
            # A column the current models no longer declare (e.g. an old `caption`
            # or `captions`). Add it as TEXT and retry instead of dropping the row.
            match = re.search(r'column "([^"]+)" of relation "([^"]+)" does not exist', str(exc))
            conn.rollback()
            if not match:
                failed += 1
                print('  skip: %s' % str(exc).splitlines()[0][:120])
                continue
            column, table = match.groups()
            try:
                cur.execute('ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS "%s" TEXT' % (table, column))
                added_columns.append('%s.%s' % (table, column))
                cur.execute(stmt)
                executed += 1
            except Exception as retry_exc:
                failed += 1
                print('  skip: %s -> %s' % (stmt.split('\n', 1)[0][:50],
                                            str(retry_exc).splitlines()[0][:110]))
        except Exception as exc:
            failed += 1
            print('  skip: %s -> %s' % (stmt.split('\n', 1)[0][:60], str(exc).splitlines()[0][:120]))

    if added_columns:
        print('added legacy columns: %s' % ', '.join(sorted(set(added_columns))))
    print('executed %d statements, %d skipped' % (executed, failed))

    for table in ('user', 'telegram_account', 'activity_log', 'scheduled_task',
                  'auto_video_post_campaign'):
        if table in existing or True:
            try:
                cur.execute('select count(*) from "%s"' % table)
                print('  %-28s %s rows' % (table, cur.fetchone()[0]))
            except Exception:
                conn.rollback()
    conn.close()


if __name__ == '__main__':
    main()
