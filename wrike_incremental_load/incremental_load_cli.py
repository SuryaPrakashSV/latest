"""CLI for the project-level incremental (SCD Type 2) status-history POC.

Two data sources feed every run:
  1. A LIVE snapshot of current project statuses, pulled from Wrike.
  2. Whatever is currently stored as "current" in the history table - either
     a CSV export of it (offline testing, no Snowflake connection needed) or
     a live Snowflake connection (--apply).

The comparison itself (incremental_load_core.compute_snapshot_changes) is
pure and already covered by tests/test_incremental_load_core.py for the
new-project / status-changed / status-unchanged / null / idempotent-rerun
cases Akash's assignment asks to be tested. This CLI is just the wiring.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path

import pandas as pd

from incremental_load_core import (
    HISTORY_TABLE_COLUMNS,
    build_snapshot_from_projects,
    compute_snapshot_changes,
)


def token() -> str:
    value = os.getenv("WRIKE_TOKEN", "").strip()
    return value or getpass.getpass("Wrike token (hidden; not saved): ").strip()


def dated_output_dir(base: str) -> Path:
    """Namespace output by UTC date, same pattern as the extraction project -
    a rerun on a later day writes to a new folder instead of overwriting
    yesterday's evidence.
    """
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d")
    path = Path(base) / stamp
    path.mkdir(parents=True, exist_ok=True)
    return path


def snowflake_connection_from_env():
    """Build a Snowflake connection from SNOWFLAKE_* env vars.

    Imported lazily so the rest of this tool works without the
    snowflake-connector-python package installed unless --apply or
    --history-from-snowflake is actually used.
    """
    import snowflake.connector

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(
            f"Missing required environment variable(s) for Snowflake: {missing}. "
            "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE "
            "(and optionally SNOWFLAKE_ROLE)."
        )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    ensure_table = sub.add_parser(
        "ensure-table",
        help="Create the WRIKE_PROJECT_STATUS_HISTORY_SCD table in the non-production sandbox schema if it doesn't exist yet.",
    )

    run = sub.add_parser(
        "run",
        help=(
            "Pull current status for the given project(s) live from Wrike, compare "
            "against stored history (--history-csv for offline testing, or live "
            "Snowflake via --apply), and report what would be inserted/closed. "
            "Add --apply to actually write the changes to Snowflake."
        ),
    )
    run.add_argument("--project-id", action="append", required=True, help="Repeat for multiple projects.")
    run.add_argument(
        "--history-csv",
        help=(
            "CSV export of the history table's current IS_CURRENT rows "
            "(HISTORY_TABLE_COLUMNS). Omit to treat every project as having "
            "no prior stored history (a first run). Ignored if --apply is set "
            "(live Snowflake is read instead)."
        ),
    )
    run.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually read from and write to Snowflake (via SNOWFLAKE_* env vars) "
            "instead of just reporting what would happen."
        ),
    )
    run.add_argument("--output", default="output/incremental_run")

    args = parser.parse_args()

    if args.command == "ensure-table":
        import snowflake_repository

        connection = snowflake_connection_from_env()
        try:
            snowflake_repository.ensure_table(connection)
            print(f"Ensured {snowflake_repository.FQN} exists.")
        finally:
            connection.close()

    elif args.command == "run":
        from wrike_status_api import WrikeClient

        client = WrikeClient(token())
        projects = [client.project(project_id) for project_id in args.project_id]
        status_names = client.status_names()
        snapshot = build_snapshot_from_projects(projects, status_names=status_names)

        connection = None
        if args.apply:
            import snowflake_repository

            connection = snowflake_connection_from_env()
            snowflake_repository.ensure_table(connection)
            existing = snowflake_repository.fetch_current_rows(connection)
        elif args.history_csv:
            existing = pd.read_csv(args.history_csv, low_memory=False)
            for column in HISTORY_TABLE_COLUMNS:
                if column not in existing.columns:
                    existing[column] = pd.NA
            existing = existing[HISTORY_TABLE_COLUMNS]
        else:
            print("No --history-csv or --apply given - treating every project as having no prior history.")
            existing = pd.DataFrame(columns=HISTORY_TABLE_COLUMNS)

        detected_at = pd.Timestamp.now(tz="UTC")
        changes = compute_snapshot_changes(
            current_snapshot=snapshot, existing_current_rows=existing, detected_at=detected_at
        )

        output = dated_output_dir(args.output)
        snapshot.to_csv(output / "snapshot.csv", index=False)
        pd.DataFrame(changes["rows_to_insert"], columns=HISTORY_TABLE_COLUMNS).to_csv(
            output / "rows_to_insert.csv", index=False
        )
        pd.DataFrame(changes["rows_to_close"]).to_csv(output / "rows_to_close.csv", index=False)

        print(f"Snapshot: {len(snapshot)} project(s) pulled live from Wrike.")
        print(f"Unchanged: {len(changes['unchanged_project_ids'])}")
        print(f"Status changed (close + insert): {len(changes['rows_to_close'])}")
        print(f"New projects (insert only): {len(changes['rows_to_insert']) - len(changes['rows_to_close'])}")

        if args.apply and connection is not None:
            import snowflake_repository

            try:
                result = snowflake_repository.apply_changes(connection, changes)
                print(f"Applied to Snowflake: {result['rows_closed']} closed, {result['rows_inserted']} inserted.")
            finally:
                connection.close()
        else:
            print(
                "\nDry run only - nothing written to Snowflake. "
                "Re-run with --apply (and SNOWFLAKE_* env vars set) to actually write these changes."
            )

        summary = {
            "detected_at": detected_at.isoformat(),
            "project_ids": args.project_id,
            "unchanged_count": len(changes["unchanged_project_ids"]),
            "closed_count": len(changes["rows_to_close"]),
            "inserted_count": len(changes["rows_to_insert"]),
            "applied": bool(args.apply),
        }
        (output / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
        print(f"\nWrote {output}/")


if __name__ == "__main__":
    main()
