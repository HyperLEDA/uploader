import csv
import io
from collections.abc import Callable
from typing import Literal, cast

from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import env_map
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import set_crossmatch_results
from uploader.clients.gen.client.adminapi.models.existing_status_payload import ExistingStatusPayload
from uploader.clients.gen.client.adminapi.models.new_status_payload import NewStatusPayload
from uploader.clients.gen.client.adminapi.models.record_triage_status import RecordTriageStatus
from uploader.clients.gen.client.adminapi.models.set_crossmatch_results_request import (
    SetCrossmatchResultsRequest,
)
from uploader.clients.gen.client.adminapi.models.statuses_payload import StatusesPayload
from uploader.clients.gen.client.adminapi.types import UNSET, Unset

_TEXTAREA = {"ui:widget": "textarea", "ui:options": {"rows": 10}}


class ResolveCrossmatchConflictsForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    new_records_text: str = Field(
        default="",
        title="New records",
        description="One record id per line; each is marked as a new object (no PGC).",
        json_schema_extra=_TEXTAREA,
    )
    existing_records_csv: str = Field(
        default="",
        title="Existing records",
        description="CSV: first column record id, second column PGC (one row per match).",
        json_schema_extra=_TEXTAREA,
    )


def _normalize_new_record_ids_text(raw: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in raw.splitlines():
        t = line.strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _parse_existing_records_csv(text: str) -> tuple[list[str], list[int]]:
    stripped = text.strip()
    if not stripped:
        return [], []
    reader = csv.reader(io.StringIO(stripped))
    rows = [r for r in reader if r and any(c.strip() for c in r)]
    if not rows:
        return [], []
    start = 0
    if len(rows[0]) >= 2:
        try:
            int(rows[0][1].strip())
        except ValueError:
            start = 1
    by_id: dict[str, int] = {}
    for i, row in enumerate(rows[start:], start=start + 1):
        if len(row) < 2:
            raise ValueError(f"Existing CSV row {i}: need at least two columns (record id, PGC).")
        rid = row[0].strip()
        if not rid:
            raise ValueError(f"Existing CSV row {i}: empty record id.")
        try:
            pgc = int(row[1].strip())
        except ValueError as e:
            raise ValueError(f"Existing CSV row {i}: invalid PGC {row[1]!r}.") from e
        if pgc <= 0:
            raise ValueError(f"Existing CSV row {i}: PGC must be positive.")
        if rid in by_id and by_id[rid] != pgc:
            raise ValueError(f"Conflicting PGC values for record_id {rid!r}.")
        by_id[rid] = pgc
    record_ids = list(by_id.keys())
    return record_ids, [by_id[r] for r in record_ids]


def handle_resolve_crossmatch_conflicts(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(ResolveCrossmatchConflictsForm, form)
    new_ids = _normalize_new_record_ids_text(f.new_records_text)
    existing_ids, existing_pgcs = _parse_existing_records_csv(f.existing_records_csv)

    if not new_ids and not existing_ids:
        raise ValueError("Provide at least one new record id or one existing override.")

    overlap = set(new_ids) & set(existing_ids)
    if overlap:
        raise ValueError(f"The following record ids appear as both new and existing: {', '.join(sorted(overlap))}")

    resolved = RecordTriageStatus.RESOLVED
    new_pl: NewStatusPayload | None = None
    if new_ids:
        triage_new = [resolved] * len(new_ids)
        new_pl = NewStatusPayload(
            record_ids=new_ids,
            triage_statuses=cast(list[RecordTriageStatus | None] | Unset, triage_new),
        )
    existing_pl: ExistingStatusPayload | None = None
    if existing_ids:
        triage_existing = [resolved] * len(existing_ids)
        existing_pl = ExistingStatusPayload(
            record_ids=existing_ids,
            pgcs=existing_pgcs,
            triage_statuses=cast(list[RecordTriageStatus | None] | Unset, triage_existing),
        )

    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    report_func(
        report.LogEvent(
            message=(
                f"Pushing crossmatch resolutions: {len(new_ids)} new, "
                f"{len(existing_ids)} existing (endpoint={f.endpoint})."
            ),
        ),
    )
    handle_call(
        set_crossmatch_results.sync_detailed(
            client=client,
            body=SetCrossmatchResultsRequest(
                statuses=StatusesPayload(
                    new=new_pl if new_pl is not None else UNSET,
                    existing=existing_pl if existing_pl is not None else UNSET,
                    collided=UNSET,
                ),
            ),
        ),
    )
    report_func(report.ProgressEvent(percent=100.0))
    report_func(
        report.DoneEvent(
            message=f"Applied crossmatch results: {len(new_ids)} new, {len(existing_ids)} existing.",
        ),
    )
