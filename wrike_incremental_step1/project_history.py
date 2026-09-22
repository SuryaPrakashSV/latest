"""Local observed project status history. Python 3.9+, standard library only.

Network access occurs ONLY for the capture command (GET selected project IDs).
No Snowflake integration, remote writes, historical backfill or scheduler.
"""
import argparse
import csv
import getpass
import hashlib
import json
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, HTTPRedirectHandler


def utc(value):
    if not isinstance(value, str):
        raise ValueError('Timestamp must be an ISO string with timezone')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Timestamp requires a timezone')
    return parsed.astimezone(timezone.utc).isoformat(timespec='microseconds')


def now():
    return utc(datetime.now(timezone.utc).isoformat())


def required(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(label + ' must be a nonempty string')
    return value.strip()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def validate(snapshot):
    if not isinstance(snapshot, dict) or snapshot.get('schema_version') != 1:
        raise ValueError('Expected snapshot schema_version 1')
    if snapshot.get('complete') is not True:
        raise ValueError('Incomplete snapshots cannot advance local history')
    source = snapshot.get('source')
    if source not in ('LIVE_WRIKE', 'SYNTHETIC_TEST'):
        raise ValueError('source must be LIVE_WRIKE or SYNTHETIC_TEST')
    run_id = required(snapshot.get('run_id'), 'run_id')
    account = required(snapshot.get('account_id'), 'account_id')
    started = utc(snapshot.get('capture_started_at'))
    finished = utc(snapshot.get('capture_finished_at'))
    if started > finished:
        raise ValueError('Capture end precedes start')
    ids = snapshot.get('requested_project_ids')
    if not isinstance(ids, list) or not ids:
        raise ValueError('A nonempty requested_project_ids list is required')
    ids = [required(i, 'project ID') for i in ids]
    if len(set(ids)) != len(ids):
        raise ValueError('Duplicate requested project IDs')
    projects = snapshot.get('projects')
    if not isinstance(projects, list):
        raise ValueError('projects must be an array')
    normalized = []
    seen = set()
    for item in projects:
        raw = item.get('data') if isinstance(item, dict) else None
        if not isinstance(raw, dict) or not isinstance(raw.get('project'), dict):
            raise ValueError('Every item must contain a project object')
        pid = required(raw.get('id'), 'project ID')
        if pid in seen:
            raise ValueError('Duplicate returned project ID: ' + pid)
        seen.add(pid)
        if required(raw.get('accountId'), 'accountId') != account:
            raise ValueError('Mixed or mismatched accounts')
        observed = utc(item.get('observed_at'))
        if not started <= observed <= finished:
            raise ValueError('Observation outside capture window')
        status = required(raw['project'].get('customStatusId'), 'project.customStatusId')
        updated = utc(raw['updatedDate']) if raw.get('updatedDate') else None
        normalized.append((pid, status, observed, updated, canonical(raw)))
    if seen != set(ids):
        raise ValueError('Returned IDs do not exactly match requested IDs')
    return run_id, account, source, started, finished, normalized


DDL = '''
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs (
 run_id TEXT PRIMARY KEY, digest TEXT NOT NULL, capture_started_at TEXT NOT NULL,
 capture_finished_at TEXT NOT NULL, summary_json TEXT NOT NULL, snapshot_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS latest (
 project_id TEXT PRIMARY KEY, custom_status_id TEXT NOT NULL,
 observed_at TEXT NOT NULL, source_updated_at TEXT, raw_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS history (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL,
 version INTEGER NOT NULL, custom_status_id TEXT NOT NULL, observed_at TEXT NOT NULL,
 source_updated_at TEXT, run_id TEXT NOT NULL REFERENCES runs(run_id),
 reason TEXT NOT NULL, raw_json TEXT NOT NULL,
 UNIQUE(project_id, version), UNIQUE(project_id, run_id));
'''


def apply_snapshot(snapshot, database):
    """Validate all inputs, then commit history/current state/run in one transaction.

One DB represents one Wrike account and one evidence source. Missing projects
are never inferred deleted. Existing tracked IDs must be present; adding IDs
is allowed. Old exact run replay is a no-op, not a stale-state rollback.
"""
    run_id, account, source, started, finished, rows = validate(snapshot)
    serialized = canonical(snapshot)
    digest = hashlib.sha256(serialized.encode()).hexdigest()
    db = Path(database)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db, timeout=30, isolation_level=None)
    con.execute('PRAGMA foreign_keys=ON')
    try:
        con.executescript(DDL)
        con.execute('BEGIN IMMEDIATE')
        for key, value in [('schema_version', '1'), ('account_id', account), ('source', source)]:
            previous = con.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
            if previous and previous[0] != value:
                raise ValueError('Database ' + key + ' mismatch; use a separate database')
            con.execute('INSERT OR IGNORE INTO metadata VALUES (?,?)', (key, value))
        old_run = con.execute('SELECT digest,summary_json FROM runs WHERE run_id=?', (run_id,)).fetchone()
        if old_run:
            if old_run[0] != digest:
                raise ValueError('Existing run ID has different contents')
            con.rollback()
            return {'run_id': run_id, 'replayed': True, 'inserted_versions': 0,
                    'original_summary': json.loads(old_run[1])}
        latest_run = con.execute('SELECT MAX(capture_finished_at) FROM runs').fetchone()[0]
        if latest_run and started <= latest_run:
            raise ValueError('Stale or overlapping capture; fetch a fresh snapshot')
        tracked = {r[0] for r in con.execute('SELECT project_id FROM latest')}
        missing = tracked - {r[0] for r in rows}
        if missing:
            raise ValueError('Snapshot omits tracked projects; history unchanged')
        summary = {'run_id': run_id, 'source': source, 'projects': len(rows),
                   'new_baselines': 0, 'status_changed': 0, 'unchanged': 0,
                   'inserted_versions': 0, 'replayed': False}
        # Placeholder summary is finalized before transaction commit.
        con.execute('INSERT INTO runs VALUES (?,?,?,?,?,?)',
                    (run_id, digest, started, finished, '{}', serialized))
        for pid, status, observed, updated, raw in rows:
            previous = con.execute('SELECT custom_status_id,source_updated_at FROM latest WHERE project_id=?',
                                   (pid,)).fetchone()
            if previous and previous[1] and updated and updated < previous[1]:
                raise ValueError('Source timestamp moved backwards for ' + pid)
            reason = 'BASELINE' if previous is None else 'OBSERVED_STATUS_CHANGE'
            if previous is None or previous[0] != status:
                version = con.execute('SELECT COALESCE(MAX(version),0)+1 FROM history WHERE project_id=?',
                                      (pid,)).fetchone()[0]
                con.execute('INSERT INTO history(project_id,version,custom_status_id,observed_at,source_updated_at,run_id,reason,raw_json) VALUES (?,?,?,?,?,?,?,?)',
                            (pid, version, status, observed, updated, run_id, reason, raw))
                summary['new_baselines' if previous is None else 'status_changed'] += 1
                summary['inserted_versions'] += 1
            else:
                summary['unchanged'] += 1
            con.execute('INSERT INTO latest VALUES (?,?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET custom_status_id=excluded.custom_status_id,observed_at=excluded.observed_at,source_updated_at=excluded.source_updated_at,raw_json=excluded.raw_json',
                        (pid, status, observed, updated, raw))
        con.execute('UPDATE runs SET summary_json=? WHERE run_id=?', (canonical(summary), run_id))
        con.commit()
        return summary
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def get_projects(ids, token):
    """One documented read-only GET, up to 100 IDs (below API cap 1000).

No automatic retries in this first step: errors leave the DB untouched.
The caller can rerun after addressing a permission/rate-limit/network error.
"""
    if not ids or len(ids) > 100 or len(set(ids)) != len(ids):
        raise ValueError('Supply 1–100 unique project IDs')
    if any(not re.fullmatch(r'[A-Za-z0-9_-]+', i) for i in ids):
        raise ValueError('Invalid project ID format')
    token = required(token, 'token')
    request = Request('https://www.wrike.com/api/v4/folders/' + ','.join(ids),
                      headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json'}, method='GET')
    try:
        with build_opener(NoRedirect()).open(request, timeout=60) as response:
            body = response.read(20 * 1024 * 1024 + 1)
    except HTTPError as exc:
        raise ValueError('Wrike HTTP ' + str(exc.code) + '; no history committed. Check access or retry later.') from None
    except (URLError, TimeoutError):
        raise ValueError('Wrike network/timeout failure; no history committed') from None
    if len(body) > 20 * 1024 * 1024:
        raise ValueError('Response exceeds 20 MiB; reduce the selected project count')
    payload = json.loads(body)
    if payload.get('kind') != 'folders' or not isinstance(payload.get('data'), list) or payload.get('nextPageToken'):
        raise ValueError('Unexpected or incomplete folder response')
    return payload['data']


def capture(ids, token):
    started = now()
    projects = get_projects(ids, token)
    observed = now()
    accounts = {p.get('accountId') for p in projects}
    if len(accounts) != 1:
        raise ValueError('Expected projects from one account')
    snapshot = {'schema_version': 1, 'run_id': str(uuid.uuid4()), 'source': 'LIVE_WRIKE',
                'complete': True, 'account_id': accounts.pop(), 'requested_project_ids': ids,
                'capture_started_at': started, 'capture_finished_at': observed,
                'projects': [{'observed_at': observed, 'data': p} for p in projects]}
    validate(snapshot)
    return snapshot


def export(database, destination):
    """Rebuild inspectable exports from committed local state, never API data.

History output is a diagnostic schema, NOT the production Wrike_STG format.
JSON fields preserve original data. Names are titles; custom statuses stay IDs.
"""
    path = Path(destination)
    path.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True)
    try:
        con.execute('BEGIN')
        for table, order in [('history', 'sequence'), ('latest', 'project_id'), ('runs', 'capture_finished_at')]:
            cursor = con.execute('SELECT * FROM ' + table + ' ORDER BY ' + order)
            with (path / (table + '.csv')).open('w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([c[0] for c in cursor.description])
                for row in cursor:
                    # CSV display protection; raw JSON in SQLite is unchanged.
                    writer.writerow(["'" + v if isinstance(v, str) and v.startswith(('=', '+', '-', '@', '\t', '\r')) else v for v in row])
        con.rollback()
    finally:
        con.close()


def synthetic_snapshot(index, status, title='Synthetic project'):
    stamp = '2026-01-01T%02d:00:00+00:00' % (index * 2)
    return {'schema_version': 1, 'run_id': 'SYNTHETIC_RUN_' + str(index),
            'source': 'SYNTHETIC_TEST', 'complete': True, 'account_id': 'SYNTHETIC_ACCOUNT',
            'requested_project_ids': ['SYNTHETIC_PROJECT'], 'capture_started_at': stamp,
            'capture_finished_at': stamp, 'projects': [{'observed_at': stamp,
            'data': {'id': 'SYNTHETIC_PROJECT', 'accountId': 'SYNTHETIC_ACCOUNT',
                     'title': title, 'updatedDate': stamp,
                     'project': {'customStatusId': status}}}]}


def demo(output):
    folder = Path(output)
    folder.mkdir(parents=True, exist_ok=True)
    db = folder / 'synthetic.sqlite'
    if db.exists():
        raise ValueError('Demo DB already exists; use a new --output directory')
    results = []
    for index, status in enumerate(['S_IN_PROGRESS', 'S_IN_PROGRESS', 'S_COMPLETED', 'S_IN_PROGRESS'], 1):
        snap = synthetic_snapshot(index, status)
        (folder / ('snapshot_' + str(index) + '.json')).write_text(json.dumps(snap, indent=2), encoding='utf-8')
        results.append(apply_snapshot(snap, db))
    results.append(apply_snapshot(snap, db))
    export(db, folder)
    (folder / 'summary.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    d = sub.add_parser('demo', help='Offline synthetic demonstration; no token/network')
    d.add_argument('--output', default='output/demo')
    c = sub.add_parser('capture', help='GET selected project snapshots and compare locally')
    c.add_argument('--project-id', action='append', required=True)
    c.add_argument('--db', default='output/live/project_history.sqlite')
    c.add_argument('--output', default='output/live/runs')
    a = sub.add_parser('apply', help='Apply one saved complete snapshot locally')
    a.add_argument('--snapshot', required=True)
    a.add_argument('--db', required=True)
    e = sub.add_parser('export', help='Export the committed local DB to diagnostic CSV files')
    e.add_argument('--db', required=True)
    e.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        if args.command == 'demo':
            print(json.dumps(demo(args.output), indent=2))
        elif args.command == 'capture':
            snap = capture(args.project_id, getpass.getpass('Wrike token (hidden; never saved): ').strip())
            directory = Path(args.output) / snap['run_id']
            directory.mkdir(parents=True, exist_ok=False)
            (directory / 'snapshot.json').write_text(json.dumps(snap, indent=2), encoding='utf-8')
            result = apply_snapshot(snap, args.db)
            (directory / 'summary.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
            print(json.dumps(result, indent=2))
            print('Evidence:', directory)
            print('Local DB:', args.db)
        elif args.command == 'apply':
            print(json.dumps(apply_snapshot(json.loads(Path(args.snapshot).read_text(encoding='utf-8')), args.db), indent=2))
        else:
            export(args.db, args.output)
            print('Exported committed local history to', args.output)
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
        print('ERROR:', str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
