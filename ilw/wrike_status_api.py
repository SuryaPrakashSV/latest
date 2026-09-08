"""Permission-aware Wrike API client for status-history backfill."""

from __future__ import annotations

import time
import json
from datetime import datetime, timezone
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

import requests


class WrikePermissionError(RuntimeError):
    pass


# Verified against https://developers.wrike.com/llms.txt and the individual
# reference pages - real URLs fetched and read, not guessed. Keyed by the
# same path prefix used in WrikeClient.get()/probe() calls. Used so any
# blocked-access message can point straight at the relevant docs instead of
# a bare status code.
ENDPOINT_DOCS: dict[str, str] = {
    "audit_log": "https://developers.wrike.com/reference/getaudit_logempty",
    "data_export": "https://developers.wrike.com/reference/getdata_exportempty",
    "webhooks": "https://developers.wrike.com/docs/webhooks",
    "workflows": "https://developers.wrike.com/reference/getworkflowsempty",
    "customfields": "https://developers.wrike.com/reference/getcustomfieldsempty",
    "folders": "https://developers.wrike.com/reference/getfoldersempty",
    "spaces": "https://developers.wrike.com/reference/getspacesempty",
    "tasks": "https://developers.wrike.com/reference/gettasksempty",
    "comments": "https://developers.wrike.com/reference/getcommentsempty",
    "approvals": "https://developers.wrike.com/reference/getapprovalsempty",
    "timelogs": "https://developers.wrike.com/reference/gettimelogsempty",
    "contacts": "https://developers.wrike.com/reference/getcontactsempty",
}


def doc_url_for(path: str) -> str | None:
    """Best-effort Wrike developer-docs link for a GET path used in this client.

    Matches on the first path segment (e.g. "folders/ABC123" -> "folders").
    Returns None rather than a guess if nothing matches - a missing link is
    honest, a wrong one is worse than no link at all.
    """
    first_segment = path.lstrip("/").split("/", 1)[0]
    return ENDPOINT_DOCS.get(first_segment)


CATALOG_COLUMNS = [
    "custom_status_id",
    "status_name",
    "status_group",
    "status_color",
    "workflow_id",
    "workflow_name",
    "status_hidden",
    "workflow_hidden",
]


def flatten_workflows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """One row per custom status, flattened out of a GET /workflows response.

    Kept as a plain function (not a method) so it's testable with a canned
    payload and no token.
    """
    rows: list[dict[str, Any]] = []
    for workflow in payload.get("data", []) or []:
        for status in workflow.get("customStatuses", []) or []:
            status_id = status.get("id")
            if not status_id:
                continue
            rows.append(
                {
                    "custom_status_id": str(status_id),
                    "status_name": str(status.get("name") or status_id),
                    "status_group": status.get("group"),
                    "status_color": status.get("color"),
                    "workflow_id": str(workflow.get("id") or ""),
                    "workflow_name": workflow.get("name"),
                    "status_hidden": bool(status.get("hidden", False)),
                    "workflow_hidden": bool(workflow.get("hidden", False)),
                }
            )
    rows.sort(key=lambda row: (str(row["workflow_name"] or ""), str(row["status_name"])))
    return rows


# dates.start/due are schedule fields a PM sets by hand, not events -- easy
# to mix up with an actual status change (createdDate/completedDate) so
# spelling it out here once instead of relying on people remembering.
FIELD_MEANINGS: dict[str, str] = {
    "createdDate": "when the object was created, set once, never changes",
    "updatedDate": "last edit of any kind (status, dates, fields...), not just status",
    "completedDate": "the one field actually tied to a real status change",
    "customStatusId": "current status id only, no history - map to a name via /workflows",
    "dates.start": "planned start date, set by a PM - not derived from an actual status change",
    "dates.due": "planned due date, same deal, not an actual completion event",
    "dates.type": "metadata about the dates block itself (Planned, Backlog, etc), not a date",
    "project.authorId": "who created it",
    "project.ownerIds": "current owner(s)",
    "project.createdDate": "same as createdDate, nested copy",
    "project.completedDate": "same as completedDate, nested copy",
    "project.customStatusId": "same as customStatusId, nested copy",
    "project.startDate": "planned start date, set by a PM - not derived from an actual status change",
    "project.endDate": "planned end date, set by a PM - not derived from an actual status change",
}

_DATE_OR_STATUS_HINT = (
    "date", "status", "changed", "when", "stage", "phase", "milestone",
    "timeline", "activity", "log", "history", "event", "transition", "modified",
)


def describe_project_fields(project: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Walk a GET /folders/{id} response and label every date/status field.

    Mainly so "is this an actual event or just a planned date someone typed
    in" has one answer instead of everyone guessing per field.
    """
    rows: list[dict[str, Any]] = []

    def visit(node: Any, path: str) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                visit(value, f"{path}.{key}" if path else str(key))
            return
        lowered = path.lower()
        if not any(hint in lowered for hint in _DATE_OR_STATUS_HINT):
            return
        meaning = FIELD_MEANINGS.get(path)
        rows.append(
            {
                "field_path": path,
                "value": node,
                "meaning": meaning or "not documented here yet, check the Wrike API docs before relying on it",
                "is_actual_event": path in {"completedDate", "project.completedDate"},
                "is_planned_schedule_value": path in {
                    "dates.start", "dates.due", "project.startDate", "project.endDate",
                },
            }
        )

    visit(project, "")
    rows.sort(key=lambda row: row["field_path"])
    return rows


def label_custom_fields(
    project: Mapping[str, Any], definitions: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Resolve a project's customFields (id/value pairs) to readable names.

    Wrike custom fields are a separate mechanism from the built-in date
    fields describe_project_fields() covers - org-defined, arbitrary, and
    invisible unless you fetch the definitions and join on id. This is what
    actually answers "did you check all the custom fields" - if a PM ever
    manually tracked something like an actual in-progress date, it would
    live here, not in createdDate/completedDate/etc.
    """
    by_id = {str(item.get("id")): item for item in definitions.get("data", []) or [] if item.get("id")}
    rows: list[dict[str, Any]] = []
    for field in project.get("customFields", []) or []:
        field_id = str(field.get("id") or "")
        definition = by_id.get(field_id, {})
        rows.append(
            {
                "field_id": field_id,
                "title": definition.get("title", "(unknown - not in /customfields definitions)"),
                "type": definition.get("type"),
                "value": field.get("value"),
            }
        )
    rows.sort(key=lambda row: str(row["title"]))
    return rows


def folder_hierarchy(project: Mapping[str, Any]) -> dict[str, Any]:
    """Extract the raw parent/child/super-parent folder IDs for a project.

    Wrike folders form a graph via parentIds/childIds/superParentIds - IDs
    only, no titles. This is the ID-only shape; feed the combined IDs into
    WrikeClient.folders_batch() and pass both to label_folder_hierarchy()
    to get the readable version. Kept as a separate step so the ID
    extraction is testable without a token.
    """
    return {
        "project_id": str(project.get("id") or ""),
        "project_name": project.get("title"),
        "parent_ids": [str(i) for i in (project.get("parentIds") or [])],
        "child_ids": [str(i) for i in (project.get("childIds") or [])],
        "super_parent_ids": [str(i) for i in (project.get("superParentIds") or [])],
    }


def label_folder_hierarchy(
    hierarchy: Mapping[str, Any], folders_payload: Mapping[str, Any]
) -> dict[str, Any]:
    """Resolve the IDs from folder_hierarchy() to titles via a GET /folders
    batch response. An ID with no matching folder in the payload (e.g. not
    shared with this token) stays visible as a bare ID instead of being
    silently dropped - the point of "complete" hierarchy is not hiding gaps.
    """
    titles = {
        str(row.get("id")): row.get("title")
        for row in folders_payload.get("data", []) or []
        if row.get("id")
    }

    def resolve(ids: list[str]) -> list[dict[str, Any]]:
        return [
            {"id": folder_id, "title": titles.get(folder_id, "(not resolvable with current access)")}
            for folder_id in ids
        ]

    return {
        "project_id": hierarchy["project_id"],
        "project_name": hierarchy["project_name"],
        "parents": resolve(hierarchy["parent_ids"]),
        "children": resolve(hierarchy["child_ids"]),
        "super_parents": resolve(hierarchy["super_parent_ids"]),
    }


class WrikeClient:
    def __init__(self, token: str, base_url: str = "https://www.wrike.com/api/v4", timeout: int = 60):
        if not token:
            raise ValueError("Wrike token is required")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"bearer {token}", "Accept": "application/json"})

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(4):
            response = self.session.get(url, params=params, timeout=self.timeout)
            if response.status_code in {401, 403}:
                doc = doc_url_for(path)
                raise WrikePermissionError(
                    f"GET /{path.lstrip('/')} returned HTTP {response.status_code}; required permission/scope is unavailable."
                    + (f" Docs: {doc}" if doc else "")
                )
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 3:
                    time.sleep(min(2 ** attempt, 8))
                    continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Wrike returned a non-object response for {path}")
            return payload
        raise RuntimeError(f"Wrike request failed after retries: {path}")

    def post(self, path: str, data: Mapping[str, Any]) -> dict[str, Any]:
        """POST form data to Wrike and return a JSON object.

        Wrike's webhook examples use form-encoded values, including JSON
        strings for arrays such as ``events`` and ``parameterisedEvents``.
        Keep this separate from get() so registration cannot accidentally put
        a callback URL or signing secret into a query string.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(4):
            response = self.session.post(url, data=dict(data), timeout=self.timeout)
            if response.status_code in {401, 403}:
                raise WrikePermissionError(
                    f"POST /{path.lstrip('/')} returned HTTP {response.status_code}; "
                    "required permission/scope is unavailable."
                )
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 3:
                    time.sleep(min(2 ** attempt, 8))
                    continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Wrike returned a non-object response for POST {path}")
            return payload
        raise RuntimeError(f"Wrike request failed after retries: POST {path}")

    def project(self, project_id: str) -> dict[str, Any]:
        rows = self.get(f"folders/{project_id}").get("data", [])
        if not rows:
            raise LookupError(f"Project not found or not shared: {project_id}")
        return rows[0]

    def folders_batch(self, folder_ids: list[str]) -> dict[str, Any]:
        """Raw GET /folders/{id1,id2,...} for a batch of IDs.

        Used to resolve parent/child/super-parent titles for
        label_folder_hierarchy(). Wrike caps batch size; callers with a very
        large hierarchy would need to chunk this, not attempted here since
        no real project has needed it yet.
        """
        if not folder_ids:
            return {"data": []}
        return self.get(f"folders/{','.join(folder_ids)}")

    def register_webhook(
        self,
        *,
        folder_id: str,
        hook_url: str,
        events: list[str] | None = None,
        fields: list[str] | None = None,
        recursive: bool = False,
        secret: str | None = None,
    ) -> dict[str, Any]:
        """Register a folder/project-scoped webhook.

        When fields are requested, Wrike requires ``parameterisedEvents``
        instead of also sending the same event through ``events``. The default
        context fields preserve the project title, custom-field values and
        project metadata in future status-change payloads.
        """
        folder_id = folder_id.strip()
        if not folder_id:
            raise ValueError("folder_id is required")

        parsed = urlparse(hook_url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("hook_url must be a complete HTTP or HTTPS URL")

        normalized_events = list(dict.fromkeys(events or ["ProjectStatusChanged"]))
        if not normalized_events or any(not str(event).strip() for event in normalized_events):
            raise ValueError("At least one non-empty webhook event is required")

        compact = lambda value: json.dumps(value, separators=(",", ":"))
        form: dict[str, Any] = {
            "hookUrl": hook_url.strip(),
            "recursive": "true" if recursive else "false",
        }
        normalized_fields = list(dict.fromkeys(fields or []))
        if normalized_fields:
            form["parameterisedEvents"] = compact(
                [
                    {"event": event, "fields": normalized_fields}
                    for event in normalized_events
                ]
            )
        else:
            form["events"] = compact(normalized_events)
        if secret:
            form["secret"] = secret

        return self.post(f"folders/{folder_id}/webhooks", form)

    def status_names(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for workflow in self.get("workflows", params={"includeSpaceWorkflow": "true"}).get("data", []):
            for status in workflow.get("customStatuses", []):
                if status.get("id"):
                    mapping[str(status["id"])] = str(status.get("name") or status["id"])
        return mapping

    def workflows_payload(self) -> dict[str, Any]:
        """Raw GET /workflows response, unmodified."""
        return self.get("workflows", params={"includeSpaceWorkflow": "true"})

    def status_catalog(self) -> list[dict[str, Any]]:
        return flatten_workflows(self.workflows_payload())

    def custom_field_definitions(self) -> dict[str, Any]:
        """Raw GET /customfields response - account-level field definitions.

        Only definitions (id, title, type), not values. Values live per-project
        under project["customFields"]; join the two with label_custom_fields().
        """
        return self.get("customfields")

    def probe(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Like get(), but returns a status report instead of raising on non-200.

        Only use this for diagnostics/access checks, not for actually fetching
        data - keeps Wrike's own error message so we're not just guessing why
        something is a 403.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        record: dict[str, Any] = {
            "endpoint": f"GET /{path.lstrip('/')}",
            "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "doc_url": doc_url_for(path),
        }
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            record.update({"status_code": None, "accessible": False, "error": str(exc)})
            return record
        record["status_code"] = response.status_code
        record["accessible"] = response.status_code == 200
        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code != 200 and isinstance(body, Mapping):
            record["wrike_error"] = body.get("error")
            record["wrike_error_description"] = body.get("errorDescription")
        elif response.status_code == 200 and isinstance(body, Mapping):
            record["response_kind"] = body.get("kind")
            data = body.get("data")
            record["rows_returned"] = len(data) if isinstance(data, list) else None
        return record

    def audit_log(
        self,
        *,
        object_ids: list[str] | None = None,
        start: str = "2024-01-01T00:00:00Z",
        end: str | None = None,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch Audit Log pages. Requires amReadOnlyAuditLog and account permission."""
        end = end or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        compact = lambda value: json.dumps(value, separators=(",", ":"))
        params: dict[str, Any] = {
            "eventDate": compact({"start": start, "end": end}),
            "operations": compact(["TaskStatusChanged"]),
            "pageSize": page_size,
        }
        if object_ids:
            params["objectIds"] = compact(object_ids[:10])
            params["objectType"] = "Project"
        output: list[dict[str, Any]] = []
        next_page: str | None = None
        while True:
            if next_page:
                params["nextPageToken"] = next_page
            payload = self.get("audit_log", params=params)
            output.extend(payload.get("data", []))
            next_page = payload.get("nextPageToken")
            if not next_page:
                break
        return output

    def bi_export_manifest(self) -> dict[str, Any]:
        """Fetch BI Export manifest. Requires dataExportFull and account permission."""
        return self.get("data_export")

    def list_webhooks(self) -> list[dict[str, Any]]:
        """Registered webhook subscriptions (id, hookUrl, status, events).

        Just subscription management, no date-range params - the `status`
        field here is the subscription's own active/suspended flag, not a
        project status, don't confuse the two when reading the response.
        """
        return self.get("webhooks").get("data", [])

    def tasks_in_folder(
        self,
        folder_id: str,
        *,
        descendants: bool = True,
        include_subtasks: bool = True,
        statuses: list[str] | None = None,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """GET /folders/{folderId}/tasks - every task Wrike will return for this folder tree.

        Verified against https://developers.wrike.com/reference/getfolderssingletasks:
        ``descendants=true`` extends the search into nested subfolders,
        ``subTasks=true`` includes subtasks in the result set. Per that same
        doc, a follow-up page request must resend the original filters
        alongside nextPageToken, not nextPageToken alone - hence params is
        reused across the loop instead of replaced.

        Critically, this endpoint does NOT return superTaskIds/subTaskIds/
        parentIds/responsibleIds/authorIds by default - they're gated behind
        the `fields` parameter (confirmed against the same doc's field list).
        Without requesting them, every task looks like a parentless top-level
        task with no assignee, which is wrong, not just incomplete. They are
        requested here explicitly for exactly that reason.

        Even with these fields, this alone is not guaranteed to reach every
        depth of a task -> subtask -> sub-subtask chain; treat it as the seed
        set and close over any subTaskIds it references but didn't return
        (see tasks_batch(), which returns these fields by default).
        """
        params: dict[str, Any] = {
            "descendants": "true" if descendants else "false",
            "subTasks": "true" if include_subtasks else "false",
            "pageSize": page_size,
            "fields": json.dumps(
                ["superTaskIds", "subTaskIds", "parentIds", "responsibleIds", "authorIds"],
                separators=(",", ":"),
            ),
        }
        if statuses:
            params["status"] = json.dumps(statuses, separators=(",", ":"))
        output: list[dict[str, Any]] = []
        next_page: str | None = None
        while True:
            if next_page:
                params["nextPageToken"] = next_page
            payload = self.get(f"folders/{folder_id}/tasks", params=params)
            output.extend(payload.get("data", []))
            next_page = payload.get("nextPageToken")
            if not next_page:
                break
        return output

    def tasks_batch(self, task_ids: list[str], *, chunk_size: int = 100) -> list[dict[str, Any]]:
        """GET /tasks/{id1,id2,...} for a batch of task IDs, chunked for safety.

        Docs (https://developers.wrike.com/reference/gettasksmulti) cap this
        at 1000 ids per call; chunking at 100 keeps well clear of that and
        of any URL-length limits on the way, at the cost of more requests.
        """
        output: list[dict[str, Any]] = []
        unique_ids = list(dict.fromkeys(task_ids))
        for start in range(0, len(unique_ids), chunk_size):
            chunk = unique_ids[start : start + chunk_size]
            if not chunk:
                continue
            output.extend(self.get(f"tasks/{','.join(chunk)}").get("data", []))
        return output

    def contacts(self) -> list[dict[str, Any]]:
        """GET /contacts - every contact (person/group/bot) in the account.

        Wrike has no id-based filter for this endpoint (confirmed against
        https://developers.wrike.com/reference/getcontactsempty), so this
        always fetches the full account roster; callers build an id -> name
        map from it rather than requesting specific ids.
        """
        return self.get("contacts").get("data", [])

    def find_projects_by_title(self, title: str) -> list[dict[str, Any]]:
        """GET /folders?title=...&project=true - find a project by name.

        Verified against https://developers.wrike.com/reference/getfoldersempty:
        `title` is a CONTAINS-match filter, not an exact-name lookup, and can
        return multiple folders. Callers must filter the result to an exact
        (case-insensitive) title match themselves if they need a specific
        project rather than every folder whose title happens to contain this
        substring.
        """
        return self.get("folders", params={"title": title, "project": "true"}).get("data", [])
