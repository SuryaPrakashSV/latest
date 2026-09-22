import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from project_history import apply_snapshot, capture, export, get_projects, synthetic_snapshot


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'state.sqlite'

    def apply(self, index, status):
        return apply_snapshot(synthetic_snapshot(index, status), self.db)

    def query(self, sql):
        with sqlite3.connect(self.db) as con:
            return con.execute(sql).fetchall()

    def test_baseline_unchanged_changed_reopened_and_replay(self):
        self.assertEqual(self.apply(1, 'A')['new_baselines'], 1)
        self.assertEqual(self.apply(2, 'A')['inserted_versions'], 0)
        self.assertEqual(self.apply(3, 'B')['status_changed'], 1)
        self.assertEqual(self.apply(4, 'A')['status_changed'], 1)
        replay = self.apply(1, 'A')
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['inserted_versions'], 0)
        self.assertEqual(self.query('SELECT custom_status_id FROM history ORDER BY sequence'), [('A',), ('B',), ('A',)])
        self.assertEqual(self.query('SELECT COUNT(*) FROM latest'), [(1,)])

    def test_metadata_edit_refreshes_latest_without_history_version(self):
        self.apply(1, 'A')
        snap = synthetic_snapshot(2, 'A', title='Renamed')
        self.assertEqual(apply_snapshot(snap, self.db)['unchanged'], 1)
        self.assertEqual(json.loads(self.query('SELECT raw_json FROM latest')[0][0])['title'], 'Renamed')
        self.assertEqual(self.query('SELECT COUNT(*) FROM history'), [(1,)])

    def test_source_timestamp_not_used_as_observed_time(self):
        snap = synthetic_snapshot(2, 'A')
        snap['projects'][0]['data']['updatedDate'] = '2020-01-01T00:00:00Z'
        apply_snapshot(snap, self.db)
        observed, updated = self.query('SELECT observed_at,source_updated_at FROM history')[0]
        self.assertTrue(observed.startswith('2026-01-01'))
        self.assertTrue(updated.startswith('2020-01-01'))

    def test_blank_or_missing_status_rejected_without_false_transition(self):
        self.apply(1, 'A')
        for value in [None, '', ' ', 1]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.apply(2, value)
        self.assertEqual(self.query('SELECT COUNT(*) FROM history'), [(1,)])

    def test_missing_or_duplicate_ids_and_plain_folder_rejected(self):
        for case in ['missing', 'duplicate', 'plain_folder', 'blank_id']:
            snap = synthetic_snapshot(1, 'A')
            if case == 'missing':
                snap['projects'] = []
            elif case == 'duplicate':
                snap['projects'] *= 2
            elif case == 'plain_folder':
                del snap['projects'][0]['data']['project']
            else:
                snap['projects'][0]['data']['id'] = ' '
            with self.subTest(case=case), self.assertRaises(ValueError):
                apply_snapshot(snap, self.db)

    def test_incomplete_snapshot_does_not_advance_run(self):
        self.apply(1, 'A')
        snap = synthetic_snapshot(2, 'B')
        snap['complete'] = False
        with self.assertRaises(ValueError):
            apply_snapshot(snap, self.db)
        self.assertEqual(self.query('SELECT COUNT(*) FROM runs'), [(1,)])

    def test_conflicting_run_id_rejected(self):
        self.apply(1, 'A')
        with self.assertRaises(ValueError):
            self.apply(1, 'B')
        self.assertEqual(self.query('SELECT custom_status_id FROM latest'), [('A',)])

    def test_older_snapshot_rejected(self):
        self.apply(2, 'B')
        with self.assertRaises(ValueError):
            self.apply(1, 'A')
        self.assertEqual(self.query('SELECT COUNT(*) FROM runs'), [(1,)])

    def test_tracked_project_omission_rejected(self):
        self.apply(1, 'A')
        snap = synthetic_snapshot(2, 'B')
        snap['requested_project_ids'] = ['OTHER']
        snap['projects'][0]['data']['id'] = 'OTHER'
        with self.assertRaises(ValueError):
            apply_snapshot(snap, self.db)
        self.assertEqual(self.query('SELECT COUNT(*) FROM runs'), [(1,)])

    def test_new_project_can_join_scope(self):
        self.apply(1, 'A')
        snap = synthetic_snapshot(2, 'A')
        item = copy.deepcopy(snap['projects'][0])
        item['data']['id'] = 'OTHER'
        snap['projects'].append(item)
        snap['requested_project_ids'].append('OTHER')
        result = apply_snapshot(snap, self.db)
        self.assertEqual((result['new_baselines'], result['unchanged']), (1, 1))

    def test_transaction_rolls_back_earlier_insert_when_later_row_is_stale(self):
        self.apply(2, 'A')
        snap = synthetic_snapshot(3, 'B')
        new = copy.deepcopy(snap['projects'][0])
        new['data']['id'] = 'NEW'
        snap['projects'].insert(0, new)
        snap['requested_project_ids'].append('NEW')
        snap['projects'][1]['data']['updatedDate'] = '2020-01-01T00:00:00Z'
        with self.assertRaises(ValueError):
            apply_snapshot(snap, self.db)
        self.assertEqual(self.query('SELECT COUNT(*) FROM runs'), [(1,)])
        self.assertEqual(self.query('SELECT COUNT(*) FROM latest'), [(1,)])
        self.assertEqual(self.query('SELECT custom_status_id FROM history'), [('A',)])

    def test_synthetic_and_live_evidence_cannot_mix(self):
        self.apply(1, 'A')
        snap = synthetic_snapshot(2, 'B')
        snap['source'] = 'LIVE_WRIKE'
        with self.assertRaises(ValueError):
            apply_snapshot(snap, self.db)

    def test_accounts_cannot_mix(self):
        self.apply(1, 'A')
        snap = synthetic_snapshot(2, 'B')
        snap['account_id'] = snap['projects'][0]['data']['accountId'] = 'OTHER_ACCOUNT'
        with self.assertRaises(ValueError):
            apply_snapshot(snap, self.db)

    def test_exports_committed_rows(self):
        self.apply(1, 'A')
        self.apply(2, 'B')
        folder = Path(self.tmp.name) / 'exports'
        export(self.db, folder)
        self.assertIn('OBSERVED_STATUS_CHANGE', (folder / 'history.csv').read_text())
        self.assertIn('BASELINE', (folder / 'history.csv').read_text())

    @patch('project_history.build_opener')
    def test_capture_uses_get_and_preserves_nested_status(self, opener):
        raw = synthetic_snapshot(1, 'A')['projects'][0]['data']
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({'kind': 'folders', 'data': [raw]}).encode()
        opener.return_value.open.return_value = response
        snap = capture(['SYNTHETIC_PROJECT'], 'fake-token-for-test')
        request = opener.return_value.open.call_args[0][0]
        self.assertEqual(request.get_method(), 'GET')
        self.assertEqual(snap['projects'][0]['data']['project']['customStatusId'], 'A')
        self.assertNotIn('fake-token-for-test', json.dumps(snap))

    @patch('project_history.build_opener')
    def test_403_has_no_secret_or_remote_body_in_error(self, opener):
        opener.return_value.open.side_effect = HTTPError('https://www.wrike.com', 403, 'fake-secret', {}, None)
        with self.assertRaisesRegex(ValueError, 'HTTP 403') as exc:
            get_projects(['P1'], 'fake-secret')
        self.assertNotIn('fake-secret', str(exc.exception))


if __name__ == '__main__':
    unittest.main()
