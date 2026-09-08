"""Project-level incremental (SCD Type 2) status history logic.

This module is deliberately side-effect free: it contains no Snowflake or
Wrike I/O. It takes two already-fetched DataFrames - a live snapshot of
current project statuses, and whatever is currently stored as "current" in
the history table - and computes exactly what to insert/close, per Akash's
spec:

    Read the latest project snapshot.
    Compare its status with the previously stored status.
    Do nothing when the status is unchanged.
    Close the existing record when the status changes.
    Insert a new status-history record.
    Store when the pipeline detected the change.
    Repeat on every ETL run.

Keeping this pure (no I/O) is what makes it testable without a live
Snowflake connection or Wrike token - see tests/test_incremental_load_core.py
for the new-project / changed-status / unchanged-status / null / idempotent
scenarios Akash's assignment explicitly asks to be tested.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


HISTORY_TABLE_COLUMNS = [
    "PROJECT_ID",
    "PROJECT_NAME",
    "STATUS_ID",
    "STATUS_NAME",
    "IS_CURRENT",
    "EFFECTIVE_FROM",
    "EFFECTIVE_TO",
    "DETECTED_AT",
    "SOURCE",
]

SNAPSHOT_COLUMNS = ["PROJECT_ID", "PROJECT_NAME", "STATUS_ID", "STATUS_NAME"]

SOURCE_LABEL = "INCREMENTAL_SNAPSHOT_COMPARE"


def _null_safe_key(value: Any) -> str:
    """Canonical, hashable form of a status id for equality comparison.

    Plain `None == None` is True but pandas NaN == NaN is False - normalizing
    both to a single string sentinel avoids that footgun so a genuinely null
    status still compares equal to itself across runs, while still comparing
    UNEQUAL to any real status id (a null -> real transition, or vice versa,
    must still register as a change, not be silently ignored).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "\x00NULL\x00"
    return str(value)


def validate_snapshot(snapshot: pd.DataFrame) -> None:
    """Fail loudly if the snapshot is missing required columns or has duplicate PROJECT_IDs.

    A snapshot is meant to be one row per project - silently picking one of
    two conflicting rows for the same PROJECT_ID would hide a real upstream
    data problem.
    """
    missing = set(SNAPSHOT_COLUMNS) - set(snapshot.columns)
    if missing:
        raise ValueError(f"Snapshot is missing required columns: {sorted(missing)}")
    duplicate_ids = snapshot["PROJECT_ID"].astype(str).value_counts()
    duplicate_ids = duplicate_ids[duplicate_ids > 1].index.tolist()
    if duplicate_ids:
        raise ValueError(
            f"Snapshot has more than one row for the same PROJECT_ID: {duplicate_ids}. "
            "Each project must appear exactly once in a snapshot."
        )


def compute_snapshot_changes(
    *,
    current_snapshot: pd.DataFrame,
    existing_current_rows: pd.DataFrame,
    detected_at: pd.Timestamp,
) -> dict[str, Any]:
    """Compare a live snapshot to the stored "current" history rows.

    `current_snapshot` - one row per project, columns PROJECT_ID/PROJECT_NAME/
    STATUS_ID/STATUS_NAME, freshly pulled from Wrike.

    `existing_current_rows` - the subset of the history table where
    IS_CURRENT=True, i.e. what the pipeline believes each project's status
    currently is. Pass an empty DataFrame (with HISTORY_TABLE_COLUMNS) for a
    brand-new table with no history yet - every project in the snapshot will
    then be treated as new.

    Returns a dict with:
      - "rows_to_close": existing current rows whose status changed (need
        EFFECTIVE_TO=detected_at, IS_CURRENT=False applied by the caller)
      - "rows_to_insert": new HISTORY_TABLE_COLUMNS-shaped rows to insert
        (new projects AND changed-status projects both produce one)
      - "unchanged_project_ids": projects where nothing needs to happen

    Running this twice with the same `current_snapshot` against the result
    of applying the first run's changes (see apply_snapshot_changes) must
    produce zero further changes - that's the idempotency guarantee Akash's
    test list asks for, and it falls out of this function doing a pure
    stored-vs-live comparison rather than tracking any run-to-run state
    itself.
    """
    validate_snapshot(current_snapshot)

    existing_by_project = {
        str(row["PROJECT_ID"]): row
        for _, row in existing_current_rows.iterrows()
        if bool(row.get("IS_CURRENT", True))
    }

    rows_to_close: list[dict[str, Any]] = []
    rows_to_insert: list[dict[str, Any]] = []
    unchanged_project_ids: list[str] = []

    for _, snap in current_snapshot.iterrows():
        project_id = str(snap["PROJECT_ID"])
        new_status_key = _null_safe_key(snap["STATUS_ID"])
        existing = existing_by_project.get(project_id)

        if existing is None:
            # New project - never had a current row before.
            rows_to_insert.append(
                {
                    "PROJECT_ID": project_id,
                    "PROJECT_NAME": snap["PROJECT_NAME"],
                    "STATUS_ID": snap["STATUS_ID"],
                    "STATUS_NAME": snap["STATUS_NAME"],
                    "IS_CURRENT": True,
                    "EFFECTIVE_FROM": detected_at,
                    "EFFECTIVE_TO": None,
                    "DETECTED_AT": detected_at,
                    "SOURCE": SOURCE_LABEL,
                }
            )
            continue

        old_status_key = _null_safe_key(existing["STATUS_ID"])
        if old_status_key == new_status_key:
            # Unchanged - do nothing. This is the branch that must dominate
            # in practice; most projects don't change status on most runs.
            unchanged_project_ids.append(project_id)
            continue

        # Status changed - close the old current row, insert a new one.
        rows_to_close.append(
            {
                "PROJECT_ID": project_id,
                "EFFECTIVE_FROM": existing["EFFECTIVE_FROM"],
                "EFFECTIVE_TO": detected_at,
                "IS_CURRENT": False,
            }
        )
        rows_to_insert.append(
            {
                "PROJECT_ID": project_id,
                "PROJECT_NAME": snap["PROJECT_NAME"],
                "STATUS_ID": snap["STATUS_ID"],
                "STATUS_NAME": snap["STATUS_NAME"],
                "IS_CURRENT": True,
                "EFFECTIVE_FROM": detected_at,
                "EFFECTIVE_TO": None,
                "DETECTED_AT": detected_at,
                "SOURCE": SOURCE_LABEL,
            }
        )

    return {
        "rows_to_close": rows_to_close,
        "rows_to_insert": rows_to_insert,
        "unchanged_project_ids": unchanged_project_ids,
    }


def apply_snapshot_changes(history: pd.DataFrame, changes: dict[str, Any]) -> pd.DataFrame:
    """Return a NEW history DataFrame with `changes` applied - pure, no I/O.

    Used both to test idempotency (apply run 1's changes, then run the
    comparison again with the same snapshot and confirm nothing further
    changes) and as the in-memory equivalent of what the Snowflake MERGE in
    snowflake_repository.py should achieve.
    """
    result = history.copy()
    for close in changes["rows_to_close"]:
        mask = (
            result["PROJECT_ID"].astype(str).eq(str(close["PROJECT_ID"]))
            & result["IS_CURRENT"].astype(bool)
        )
        result.loc[mask, "EFFECTIVE_TO"] = close["EFFECTIVE_TO"]
        result.loc[mask, "IS_CURRENT"] = False

    if changes["rows_to_insert"]:
        new_rows = pd.DataFrame(changes["rows_to_insert"], columns=HISTORY_TABLE_COLUMNS)
        result = pd.concat([result, new_rows], ignore_index=True)

    return result


def build_snapshot_from_projects(
    projects: list[dict[str, Any]], *, status_names: dict[str, str]
) -> pd.DataFrame:
    """Build a SNAPSHOT_COLUMNS-shaped DataFrame from raw Wrike project objects.

    `projects` are raw GET /folders/{id} responses (one dict per project);
    `status_names` maps customStatusId -> readable name, from
    WrikeClient.status_names(). Every value here traces back to Wrike - no
    status or date is authored locally.
    """
    rows = []
    for project in projects:
        status_id = project.get("customStatusId")
        rows.append(
            {
                "PROJECT_ID": str(project.get("id") or ""),
                "PROJECT_NAME": project.get("title"),
                "STATUS_ID": status_id,
                "STATUS_NAME": status_names.get(str(status_id or ""), status_id),
            }
        )
    return pd.DataFrame(rows, columns=SNAPSHOT_COLUMNS)
