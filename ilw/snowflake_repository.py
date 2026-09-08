"""Snowflake persistence for the incremental (SCD Type 2) project-status history.

This targets the same non-production sandbox schema as the older full-load
POC (ASO_OPS_WSA_SNDBX_BIZ_APP), under a NEW table name so this SCD-based
table can be validated independently without touching or colliding with the
existing full-load history table.

Every function here takes a `connection` object the caller constructs
(e.g. via `snowflake.connector.connect(...)`) - this module never reads
credentials or connects itself, matching the original snowflake_repository.py
pattern: credential handling stays entirely in the caller's hands.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from incremental_load_core import HISTORY_TABLE_COLUMNS

DATABASE = "GBI_RETAIL_BAP_DB"
SCHEMA = "ASO_OPS_WSA_SNDBX_BIZ_APP"
TABLE = "WRIKE_PROJECT_STATUS_HISTORY_SCD"
FQN = f"{DATABASE}.{SCHEMA}.{TABLE}"


def ensure_table(connection) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {FQN} (
      PROJECT_ID VARCHAR NOT NULL,
      PROJECT_NAME VARCHAR,
      STATUS_ID VARCHAR,
      STATUS_NAME VARCHAR,
      IS_CURRENT BOOLEAN NOT NULL,
      EFFECTIVE_FROM TIMESTAMP_TZ NOT NULL,
      EFFECTIVE_TO TIMESTAMP_TZ,
      DETECTED_AT TIMESTAMP_TZ NOT NULL,
      SOURCE VARCHAR NOT NULL
    )
    """
    with connection.cursor() as cursor:
        cursor.execute(ddl)


def fetch_current_rows(connection) -> pd.DataFrame:
    """SELECT the IS_CURRENT=TRUE row per project - the "previously stored
    status" side of the comparison. Empty (but correctly-shaped) result on a
    fresh table, which compute_snapshot_changes treats as "every project is new".
    """
    query = f"SELECT {', '.join(HISTORY_TABLE_COLUMNS)} FROM {FQN} WHERE IS_CURRENT = TRUE"
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    frame = pd.DataFrame(rows, columns=columns)
    for column in HISTORY_TABLE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[HISTORY_TABLE_COLUMNS]


def apply_changes(connection, changes: dict[str, Any]) -> dict[str, int]:
    """Apply rows_to_close (UPDATE) then rows_to_insert (INSERT) inside one
    transaction, so a run either fully lands or fully rolls back - a partial
    apply (rows closed but the replacement never inserted) would silently
    lose the "current status" for that project.
    """
    closed = 0
    inserted = 0
    with connection.cursor() as cursor:
        try:
            for close in changes["rows_to_close"]:
                cursor.execute(
                    f"""
                    UPDATE {FQN}
                    SET EFFECTIVE_TO = %(effective_to)s, IS_CURRENT = FALSE
                    WHERE PROJECT_ID = %(project_id)s
                      AND EFFECTIVE_FROM = %(effective_from)s
                      AND IS_CURRENT = TRUE
                    """,
                    {
                        "effective_to": close["EFFECTIVE_TO"],
                        "project_id": close["PROJECT_ID"],
                        "effective_from": close["EFFECTIVE_FROM"],
                    },
                )
                closed += cursor.rowcount or 0

            for insert in changes["rows_to_insert"]:
                cursor.execute(
                    f"""
                    INSERT INTO {FQN}
                        (PROJECT_ID, PROJECT_NAME, STATUS_ID, STATUS_NAME,
                         IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO, DETECTED_AT, SOURCE)
                    VALUES
                        (%(PROJECT_ID)s, %(PROJECT_NAME)s, %(STATUS_ID)s, %(STATUS_NAME)s,
                         %(IS_CURRENT)s, %(EFFECTIVE_FROM)s, %(EFFECTIVE_TO)s, %(DETECTED_AT)s, %(SOURCE)s)
                    """,
                    insert,
                )
                inserted += 1

            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {"rows_closed": closed, "rows_inserted": inserted}
