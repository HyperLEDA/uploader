from collections.abc import Callable

import uploader.app.report as report
from uploader.clients.client import call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)

NOTE_COLUMNS = ["note"]


def upload_note(
    client: adminapi.AuthenticatedClient,
    record_id: str,
    note: str,
    report_func: Callable[[report.Event], None],
) -> None:
    call(
        client,
        SaveStructuredDataRequest(
            catalog="note",
            columns=NOTE_COLUMNS,
            ids=[record_id],
            data=[[note]],
        ),
        save_structured_data.sync_detailed,
        callback_func=lambda m: report_func(report.LogEvent(message=m)),
    )
    report_func(report.ProgressEvent(percent=100))
    report_func(report.DoneEvent(message="Note uploaded successfully."))
