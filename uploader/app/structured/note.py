from collections.abc import Callable

import uploader.app.report as report
from uploader.app.upload import handle_call
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
    handle_call(
        save_structured_data.sync_detailed(
            client=client,
            body=SaveStructuredDataRequest(
                catalog="note",
                columns=NOTE_COLUMNS,
                ids=[record_id],
                data=[[note]],
            ),
        )
    )
    report_func(report.ProgressEvent(percent=100))
    report_func(report.DoneEvent(message="Note uploaded successfully."))
