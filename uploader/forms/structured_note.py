from collections.abc import Callable
from typing import cast

from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import env_map
from uploader.app.structured.note import upload_note as run_upload_note
from uploader.clients.gen.client import adminapi


class StructuredNoteForm(BaseModel):
    record_id: str = Field(..., title="Record ID")
    note: str = Field(..., title="Note text")


def handle_structured_note(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(StructuredNoteForm, form)
    client = adminapi.AuthenticatedClient(
        base_url=env_map["prod"],
        token="fake",
    )
    run_upload_note(
        client=client,
        record_id=f.record_id.strip(),
        note=f.note,
        report_func=report_func,
    )
