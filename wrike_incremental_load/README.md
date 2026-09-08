# Wrike Incremental Load (Project-Level SCD)

Separate, standalone project for the project-level incremental/SCD status
history pipeline. This is the work Akash described as coming *after* the
task-inventory investigation (which lives in the other project,
`full_implementation`) - this project does not modify or depend on that one
at runtime; `wrike_status_api.py` is copied here rather than imported across
directories, so this project can be handed off, zipped, or run on its own.

## What this does

Each run:

1. Pulls the **current** status of one or more projects live from Wrike
   (`GET /folders/{id}`, `GET /workflows`).
2. Compares it against whatever is currently stored as "current" in the
   history table.
3. Does nothing if the status hasn't changed.
4. If it changed (or the project is new): closes the old "current" row
   (`EFFECTIVE_TO` / `IS_CURRENT = FALSE`) and inserts a new one.
5. Records when the pipeline detected the change (`DETECTED_AT`), separate
   from when the status actually took effect (`EFFECTIVE_FROM`).

This is the exact algorithm Akash specified:

> Read the latest project snapshot. Compare its status with the previously
> stored status. Do nothing when the status is unchanged. Close the existing
> record when the status changes. Insert a new status-history record. Store
> when the pipeline detected the change. Repeat on every ETL run.

## Files

| File | Purpose |
|---|---|
| `wrike_status_api.py` | Wrike API client (copied from the verified `full_implementation` project) |
| `incremental_load_core.py` | The comparison/apply logic itself - pure Python, no I/O, fully unit-testable |
| `snowflake_repository.py` | Snowflake DDL + read/write, targeting the non-production sandbox schema |
| `incremental_load_cli.py` | Command-line entry point tying the above together |
| `tests/test_incremental_load_core.py` | New project / status changed / unchanged / null / idempotent-rerun tests |

## Setup

```bash
pip install -r requirements.txt
```

## Running the tests

```bash
python3 -m unittest discover -s tests -v
```

## Running a dry run (no Snowflake needed)

Pulls live current status from Wrike, compares against "no prior history"
(or a CSV you provide), and reports what *would* be inserted/closed without
writing anything:

```bash
export WRIKE_TOKEN=your-token-here
python3 incremental_load_cli.py run --project-id MQAAAAEGPpOb --output output/incremental_run
```

To compare against an existing history table's current state without a live
Snowflake connection, export its `IS_CURRENT = TRUE` rows to CSV first
(columns: `PROJECT_ID, PROJECT_NAME, STATUS_ID, STATUS_NAME, IS_CURRENT,
EFFECTIVE_FROM, EFFECTIVE_TO, DETECTED_AT, SOURCE`), then:

```bash
python3 incremental_load_cli.py run --project-id MQAAAAEGPpOb --history-csv current_history.csv
```

## Running against non-production Snowflake

Set the connection details as environment variables (never pass credentials
on the command line or hardcode them):

```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_WAREHOUSE=...
export SNOWFLAKE_ROLE=...   # optional
```

Create the table once:

```bash
python3 incremental_load_cli.py ensure-table
```

Then run for real, reading and writing live Snowflake state:

```bash
python3 incremental_load_cli.py run --project-id MQAAAAEGPpOb --apply
```

Table: `GBI_RETAIL_BAP_DB.ASO_OPS_WSA_SNDBX_BIZ_APP.WRIKE_PROJECT_STATUS_HISTORY_SCD`
(sandbox schema, a new table name so this doesn't collide with the older
full-load `_NEW` table already in that schema).

## What's intentionally out of scope here

- Task-level status history - Wrike's Task API only exposes current status
  (`customStatusId`), confirmed via `classify-task-history` in the other
  project. This pipeline is project-level, matching Akash's own framing and
  `PROJECT_ID`-keyed comparison.
- Automatically discovering *which* projects to run against - `--project-id`
  is explicit and repeatable; wiring this into a scheduled job over a full
  list of DCM projects is a follow-up once this is validated.
