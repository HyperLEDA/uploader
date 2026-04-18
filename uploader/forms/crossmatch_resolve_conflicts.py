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


class ExistingCrossmatchOverride(BaseModel):
    record_id: str = Field(..., title="Record ID", min_length=1)
    pgc: int = Field(..., title="PGC", gt=0)


class ResolveCrossmatchConflictsForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    new_record_ids: list[str] = Field(
        default_factory=list,
        title="New records",
        description="Record ids to mark as new objects (no PGC).",
    )
    existing_overrides: list[ExistingCrossmatchOverride] = Field(
        default_factory=list,
        title="Existing-record overrides",
        description="Each entry links a record id to the chosen existing PGC.",
    )


def _normalize_new_record_ids(raw: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for s in raw:
        t = s.strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _merge_existing_overrides(
    overrides: list[ExistingCrossmatchOverride],
) -> tuple[list[str], list[int]]:
    by_id: dict[str, int] = {}
    for o in overrides:
        rid = o.record_id.strip()
        if not rid:
            raise ValueError("Existing override has empty record_id.")
        if rid in by_id and by_id[rid] != o.pgc:
            raise ValueError(f"Conflicting PGC values for record_id {rid!r}.")
        by_id[rid] = o.pgc
    record_ids = list(by_id.keys())
    return record_ids, [by_id[r] for r in record_ids]


def handle_resolve_crossmatch_conflicts(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(ResolveCrossmatchConflictsForm, form)
    new_ids = _normalize_new_record_ids(f.new_record_ids)
    existing_ids, existing_pgcs = _merge_existing_overrides(f.existing_overrides)

    if not new_ids and not existing_ids:
        raise ValueError("Provide at least one new record id or one existing override.")

    overlap = set(new_ids) & set(existing_ids)
    if overlap:
        raise ValueError(
            "The following record ids appear as both new and existing: "
            f"{', '.join(sorted(overlap))}"
        )

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
