# Wrike incremental exploration — step 1

Created 2026-09-21. Scope: project status comparison with persistent LOCAL history.
Python 3.9 or newer; standard library only. No pip install required.

## Start here: offline demonstration

Extract this package into its own folder. In Terminal, change to that folder:

```bash
cd ~/Downloads/wrike_incremental_step1
python3 project_history.py demo --output output/demo
```

Adjust the directory if your browser extracted it elsewhere. This command needs
no token and makes no network requests. All demo data is explicitly synthetic.

Expected results in order:

| Run | Input | New baselines | Status changes | Unchanged | Versions inserted |
|---|---|---:|---:|---:|---:|
| 1 | In Progress | 1 | 0 | 0 | 1 |
| 2 | In Progress | 0 | 0 | 1 | 0 |
| 3 | Completed | 0 | 1 | 0 | 1 |
| 4 | In Progress again | 0 | 1 | 0 | 1 |
| Replay | Exact run 4 again | — | — | — | 0 |

Open `output/demo/history.csv`: three versions, with raw synthetic status IDs.
`latest.csv` has one current project. `summary.json` has the per-run counts.
To rerun the demo, choose a NEW output directory, such as `output/demo_2`.
The command refuses to replace an existing demo database.

## Selected real projects: first live validation

After the demo works, choose actual project API IDs from your existing extraction
or Snowflake project column. A task ID, permalink, or numeric browser ID is not a
substitute. Select ongoing projects if possible and keep tracking the same IDs
after completion. This bounded first step supports up to 100 selected projects;
it does not yet discover the whole account or fetch tasks.

Replace `YOUR_PROJECT_API_ID` in the command:

```bash
python3 project_history.py capture --project-id YOUR_PROJECT_API_ID
```

Repeat `--project-id` to include more projects. The token prompt is hidden; paste
the token at the prompt, never inside this command or a chat message. No token is
saved. The only external operation is GET `/api/v4/folders/{folderIds}`:
https://developers.wrike.com/reference/getfoldersmulti

This uses the standard `www.wrike.com` host and normal system HTTPS settings.
Apple proxy/certificate requirements and your live response have not been tested
here. Redirects are rejected. HTTP/network errors abort; there are no automatic
retries in this version. A 429 needs a later retry respecting Wrike's limits.

Run the same command again after the next observation interval. It automatically
uses `output/live/project_history.sqlite`; do not delete or change that DB path
between observations. The first run records a baseline, not a proven transition.
An unchanged live status is a valid result, not evidence the code failed.

Exports are rebuilt from the committed database with:

```bash
python3 project_history.py export --db output/live/project_history.sqlite --output output/live/export
```

Each capture also saves `snapshot.json` and `summary.json` under a unique run
directory. If a snapshot was saved but applying it failed, inspect the error;
the local DB transaction is rolled back. A valid saved snapshot can be applied:

```bash
python3 project_history.py apply --snapshot PATH_TO_SNAPSHOT_JSON --db output/live/project_history.sqlite
```

The DB also stores every successfully applied snapshot, so exports can be
regenerated after an export failure. Exact run replay inserts zero versions.

## What this implements

- Same observed-status comparison principle as the earlier incremental POC.
- Persistent local SQLite state; no manually supplied previous-history CSV.
- Nested `project.customStatusId` is the comparison key, retained as a raw ID.
- Project status names are not fetched in this step. A workflow rename does not
  create a status transition. No name is guessed from a general status.
- Local history is append-only; `latest` is updated even on non-status edits.
- One status version per logical project, before any owner/date/hierarchy fanout.
- Separate `observed_at` and source `updatedDate`; no fabricated transition time.
- Exact requested-ID coverage, project shape, account and timestamp validation.
- Reject missing/blank custom statuses instead of calling them valid transitions.
- Reject omitted tracked IDs rather than inferring deletion or silently narrowing
  the sample. Adding new IDs is supported if all previously tracked IDs remain.
- Atomic SQLite commit of history, latest state, run evidence and summary.
- Serialize local writes; reject old/overlapping snapshots and conflicting run IDs.
- Keep synthetic evidence and live evidence in separate databases.

CSV files are diagnostic exports, not input files for Wrike_STG. Raw project data
is preserved in JSON inside the database and captures. Spreadsheet formula-like
CSV strings receive a leading apostrophe for display; the database stays exact.

## What this does NOT implement yet

This is the first executable component, not the finished incremental ETL.

- Full account discovery and project scope parity with the deployed pipeline.
- Task, subtask or deeper hierarchy extraction and comparison.
- Existing Wrike_STG columns, hierarchy flattening, owner/date expansion or seven
  downstream query compatibility. Those are explicit integration requirements.
- Delta extraction with `updatedDate`, checkpoint overlap, pagination, periodic
  reconciliation or scheduled two-hour execution.
- Production Snowflake writes, DDL, RIO changes or automatic webhook registration.
- Historical backfill or guaranteed capture of intermediate states between polls.

Only custom status changes create project history versions. Changes to dates,
owners and custom fields remain available in current/raw snapshots; whether they
should create additional business history versions remains a design decision.

We will first prove full-snapshot comparisons, then optimize extraction. This
version's last successful capture is not advertised as an API delta watermark.
Polling A → B → A between two observations of A still emits no transition.

## Verification completed here

```bash
python3 -m unittest discover -s tests -v
```

On 2026-09-21: 16 tests passed, zero failures/errors. Tests cover persistent
baseline/change/reopening/replay, raw metadata refresh, time semantics, malformed
status/identity, incomplete captures, stale data, scope/account/source isolation,
transaction rollback after an earlier insert, exports and mocked GET/403 behavior.
The runnable demo produced three history versions, one latest row and four
committed runs. Supplied `validation/demo` files are synthetic test evidence only.
No live token, Wrike request, Snowflake connection or production change was used.

## Review of the earlier implementation

Reviewed the saved `Wrike_Incremental_Load_Project.zip` (September 10 package)
and the available later CLI source. They already implement project status
comparison. This separate prototype retains that behavior while changing local
persistence and evidence handling. It does not replace your existing files.

The older core allows blank identity, does not validate duplicate current history
rows and uses incomplete null normalization. Its local CLI starts from empty
history when no CSV is supplied and uses date-only output folders. Those patterns
are unsuitable for unattended repeated observation without correction.

The current production extraction version/commit is still to be confirmed. Its
existing transformation contract must be integrated explicitly; passing these
local tests does not prove production parity or throughput.

## Next integration gates

1. Run this demo on the Mac, then capture the same real project IDs over time.
2. Verify ID/status/date attribution against the raw saved response each time.
3. Confirm deployed extraction commit and exact output schema/grain; build the
   output adapter using the established complete project/task context.
4. Expand project discovery, test task hierarchy coverage and capture all statuses
   for tracked tasks rather than repeatedly filtering only In Progress.
5. Validate delta extraction against complete snapshots and exercise failure
   recovery before scheduling. Measure unique entities separately from expanded rows.
6. Review observed-change counts, limitations and reporting impact with Akash.
   Production integration requires the stakeholder alignment he described.
