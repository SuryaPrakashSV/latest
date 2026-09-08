"""Tests for the incremental (SCD Type 2) comparison logic.

Covers exactly the scenarios Akash's assignment lists under step 5:
"Test new projects, changed statuses, nulls and duplicate runs" - plus
unchanged statuses (the "do nothing" branch, which is the one that must
dominate in real operation) and basic input validation.
"""

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from incremental_load_core import (
    HISTORY_TABLE_COLUMNS,
    apply_snapshot_changes,
    build_snapshot_from_projects,
    compute_snapshot_changes,
    validate_snapshot,
)


def empty_history() -> pd.DataFrame:
    return pd.DataFrame(columns=HISTORY_TABLE_COLUMNS)


def snapshot(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["PROJECT_ID", "PROJECT_NAME", "STATUS_ID", "STATUS_NAME"])


class IncrementalLoadCoreTests(unittest.TestCase):
    def test_new_project_is_inserted_with_no_close(self):
        detected_at = pd.Timestamp("2026-09-08T10:00:00Z")
        snap = snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S1", "STATUS_NAME": "In Progress"}])

        changes = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=empty_history(), detected_at=detected_at)

        self.assertEqual(changes["rows_to_close"], [])
        self.assertEqual(len(changes["rows_to_insert"]), 1)
        inserted = changes["rows_to_insert"][0]
        self.assertEqual(inserted["PROJECT_ID"], "P1")
        self.assertTrue(inserted["IS_CURRENT"])
        self.assertEqual(inserted["EFFECTIVE_FROM"], detected_at)
        self.assertIsNone(inserted["EFFECTIVE_TO"])
        self.assertEqual(changes["unchanged_project_ids"], [])

    def test_unchanged_status_produces_no_rows(self):
        first_seen = pd.Timestamp("2026-09-01T00:00:00Z")
        detected_at = pd.Timestamp("2026-09-08T10:00:00Z")
        existing = pd.DataFrame(
            [
                {
                    "PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S1", "STATUS_NAME": "In Progress",
                    "IS_CURRENT": True, "EFFECTIVE_FROM": first_seen, "EFFECTIVE_TO": None,
                    "DETECTED_AT": first_seen, "SOURCE": "INCREMENTAL_SNAPSHOT_COMPARE",
                }
            ],
            columns=HISTORY_TABLE_COLUMNS,
        )
        snap = snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S1", "STATUS_NAME": "In Progress"}])

        changes = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=existing, detected_at=detected_at)

        self.assertEqual(changes["rows_to_close"], [])
        self.assertEqual(changes["rows_to_insert"], [])
        self.assertEqual(changes["unchanged_project_ids"], ["P1"])

    def test_status_change_closes_old_row_and_inserts_new_one(self):
        first_seen = pd.Timestamp("2026-09-01T00:00:00Z")
        detected_at = pd.Timestamp("2026-09-08T10:00:00Z")
        existing = pd.DataFrame(
            [
                {
                    "PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S1", "STATUS_NAME": "In Progress",
                    "IS_CURRENT": True, "EFFECTIVE_FROM": first_seen, "EFFECTIVE_TO": None,
                    "DETECTED_AT": first_seen, "SOURCE": "INCREMENTAL_SNAPSHOT_COMPARE",
                }
            ],
            columns=HISTORY_TABLE_COLUMNS,
        )
        snap = snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S2", "STATUS_NAME": "Completed"}])

        changes = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=existing, detected_at=detected_at)

        self.assertEqual(len(changes["rows_to_close"]), 1)
        closed = changes["rows_to_close"][0]
        self.assertEqual(closed["PROJECT_ID"], "P1")
        self.assertEqual(closed["EFFECTIVE_FROM"], first_seen)
        self.assertEqual(closed["EFFECTIVE_TO"], detected_at)
        self.assertFalse(closed["IS_CURRENT"])

        self.assertEqual(len(changes["rows_to_insert"]), 1)
        inserted = changes["rows_to_insert"][0]
        self.assertEqual(inserted["STATUS_ID"], "S2")
        self.assertEqual(inserted["STATUS_NAME"], "Completed")
        self.assertEqual(inserted["EFFECTIVE_FROM"], detected_at)
        self.assertTrue(inserted["IS_CURRENT"])

        self.assertEqual(changes["unchanged_project_ids"], [])

    def test_null_to_real_status_is_a_change_not_ignored(self):
        first_seen = pd.Timestamp("2026-09-01T00:00:00Z")
        detected_at = pd.Timestamp("2026-09-08T10:00:00Z")
        existing = pd.DataFrame(
            [
                {
                    "PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": None, "STATUS_NAME": None,
                    "IS_CURRENT": True, "EFFECTIVE_FROM": first_seen, "EFFECTIVE_TO": None,
                    "DETECTED_AT": first_seen, "SOURCE": "INCREMENTAL_SNAPSHOT_COMPARE",
                }
            ],
            columns=HISTORY_TABLE_COLUMNS,
        )
        snap = snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S1", "STATUS_NAME": "In Progress"}])

        changes = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=existing, detected_at=detected_at)

        self.assertEqual(len(changes["rows_to_close"]), 1, "null -> real status must be detected as a change")
        self.assertEqual(len(changes["rows_to_insert"]), 1)
        self.assertEqual(changes["rows_to_insert"][0]["STATUS_ID"], "S1")

    def test_null_status_unchanged_is_not_a_false_change(self):
        """NaN == NaN is False in pandas - confirm that footgun doesn't cause
        a null status to be misreported as changing every single run."""
        first_seen = pd.Timestamp("2026-09-01T00:00:00Z")
        detected_at = pd.Timestamp("2026-09-08T10:00:00Z")
        existing = pd.DataFrame(
            [
                {
                    "PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": None, "STATUS_NAME": None,
                    "IS_CURRENT": True, "EFFECTIVE_FROM": first_seen, "EFFECTIVE_TO": None,
                    "DETECTED_AT": first_seen, "SOURCE": "INCREMENTAL_SNAPSHOT_COMPARE",
                }
            ],
            columns=HISTORY_TABLE_COLUMNS,
        )
        snap = snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": None, "STATUS_NAME": None}])

        changes = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=existing, detected_at=detected_at)

        self.assertEqual(changes["rows_to_close"], [])
        self.assertEqual(changes["rows_to_insert"], [])
        self.assertEqual(changes["unchanged_project_ids"], ["P1"])

    def test_duplicate_run_is_idempotent(self):
        """Running the exact same snapshot twice in a row - once against
        empty history, then again against the result of applying the first
        run - must produce zero further changes the second time."""
        detected_at_1 = pd.Timestamp("2026-09-08T10:00:00Z")
        snap = snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Project One", "STATUS_ID": "S1", "STATUS_NAME": "In Progress"}])

        first_run = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=empty_history(), detected_at=detected_at_1)
        self.assertEqual(len(first_run["rows_to_insert"]), 1)

        history_after_first_run = apply_snapshot_changes(empty_history(), first_run)
        self.assertEqual(len(history_after_first_run), 1)

        detected_at_2 = pd.Timestamp("2026-09-08T11:00:00Z")
        second_run = compute_snapshot_changes(
            current_snapshot=snap, existing_current_rows=history_after_first_run, detected_at=detected_at_2
        )

        self.assertEqual(second_run["rows_to_close"], [], "re-running the same snapshot must not close anything")
        self.assertEqual(second_run["rows_to_insert"], [], "re-running the same snapshot must not insert anything")
        self.assertEqual(second_run["unchanged_project_ids"], ["P1"])

    def test_multiple_status_changes_over_several_runs_build_correct_history(self):
        """Simulates a project moving Planning -> In Progress -> Completed
        across three separate ETL runs, confirming apply_snapshot_changes
        chains correctly and only one row is ever IS_CURRENT=True."""
        history = empty_history()

        run_1_time = pd.Timestamp("2026-01-01T00:00:00Z")
        run_1_changes = compute_snapshot_changes(
            current_snapshot=snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Proj", "STATUS_ID": "PLANNING", "STATUS_NAME": "Planning"}]),
            existing_current_rows=history,
            detected_at=run_1_time,
        )
        history = apply_snapshot_changes(history, run_1_changes)

        run_2_time = pd.Timestamp("2026-02-01T00:00:00Z")
        run_2_changes = compute_snapshot_changes(
            current_snapshot=snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Proj", "STATUS_ID": "INPROGRESS", "STATUS_NAME": "In Progress"}]),
            existing_current_rows=history[history["IS_CURRENT"]],
            detected_at=run_2_time,
        )
        history = apply_snapshot_changes(history, run_2_changes)

        run_3_time = pd.Timestamp("2026-03-01T00:00:00Z")
        run_3_changes = compute_snapshot_changes(
            current_snapshot=snapshot([{"PROJECT_ID": "P1", "PROJECT_NAME": "Proj", "STATUS_ID": "DONE", "STATUS_NAME": "Completed"}]),
            existing_current_rows=history[history["IS_CURRENT"]],
            detected_at=run_3_time,
        )
        history = apply_snapshot_changes(history, run_3_changes)

        self.assertEqual(len(history), 3, "three distinct statuses over time -> three history rows")
        current_rows = history[history["IS_CURRENT"] == True]  # noqa: E712
        self.assertEqual(len(current_rows), 1, "exactly one row must be IS_CURRENT at any time")
        self.assertEqual(current_rows.iloc[0]["STATUS_ID"], "DONE")
        ordered = history.sort_values("EFFECTIVE_FROM")
        self.assertListEqual(list(ordered["STATUS_ID"]), ["PLANNING", "INPROGRESS", "DONE"])
        self.assertEqual(ordered.iloc[0]["EFFECTIVE_TO"], run_2_time)
        self.assertEqual(ordered.iloc[1]["EFFECTIVE_TO"], run_3_time)
        self.assertIsNone(ordered.iloc[2]["EFFECTIVE_TO"])

    def test_two_projects_independent_new_and_changed_in_same_run(self):
        first_seen = pd.Timestamp("2026-09-01T00:00:00Z")
        detected_at = pd.Timestamp("2026-09-08T10:00:00Z")
        existing = pd.DataFrame(
            [
                {
                    "PROJECT_ID": "P1", "PROJECT_NAME": "Existing Project", "STATUS_ID": "S1", "STATUS_NAME": "In Progress",
                    "IS_CURRENT": True, "EFFECTIVE_FROM": first_seen, "EFFECTIVE_TO": None,
                    "DETECTED_AT": first_seen, "SOURCE": "INCREMENTAL_SNAPSHOT_COMPARE",
                }
            ],
            columns=HISTORY_TABLE_COLUMNS,
        )
        snap = snapshot(
            [
                {"PROJECT_ID": "P1", "PROJECT_NAME": "Existing Project", "STATUS_ID": "S2", "STATUS_NAME": "Completed"},
                {"PROJECT_ID": "P2", "PROJECT_NAME": "Brand New Project", "STATUS_ID": "S1", "STATUS_NAME": "In Progress"},
            ]
        )

        changes = compute_snapshot_changes(current_snapshot=snap, existing_current_rows=existing, detected_at=detected_at)

        self.assertEqual(len(changes["rows_to_close"]), 1)
        self.assertEqual(changes["rows_to_close"][0]["PROJECT_ID"], "P1")
        self.assertEqual({row["PROJECT_ID"] for row in changes["rows_to_insert"]}, {"P1", "P2"})
        self.assertEqual(changes["unchanged_project_ids"], [])

    def test_validate_snapshot_rejects_duplicate_project_ids(self):
        bad_snapshot = snapshot(
            [
                {"PROJECT_ID": "P1", "PROJECT_NAME": "A", "STATUS_ID": "S1", "STATUS_NAME": "In Progress"},
                {"PROJECT_ID": "P1", "PROJECT_NAME": "A duplicate row", "STATUS_ID": "S2", "STATUS_NAME": "Completed"},
            ]
        )
        with self.assertRaises(ValueError):
            validate_snapshot(bad_snapshot)

    def test_validate_snapshot_rejects_missing_columns(self):
        incomplete = pd.DataFrame([{"PROJECT_ID": "P1"}])
        with self.assertRaises(ValueError):
            validate_snapshot(incomplete)

    def test_build_snapshot_from_projects_uses_only_live_wrike_fields(self):
        projects = [
            {"id": "P1", "title": "Project One", "customStatusId": "S1"},
            {"id": "P2", "title": "Project Two", "customStatusId": None},
        ]
        status_names = {"S1": "In Progress"}

        snap = build_snapshot_from_projects(projects, status_names=status_names)

        self.assertEqual(len(snap), 2)
        row1 = snap[snap["PROJECT_ID"] == "P1"].iloc[0]
        self.assertEqual(row1["STATUS_NAME"], "In Progress")
        row2 = snap[snap["PROJECT_ID"] == "P2"].iloc[0]
        self.assertTrue(pd.isna(row2["STATUS_ID"]), "a project with no customStatusId must not get a fabricated status")


if __name__ == "__main__":
    unittest.main()
