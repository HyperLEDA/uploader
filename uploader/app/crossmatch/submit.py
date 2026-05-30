from collections.abc import Callable

from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app.storage import PgStorage
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import assign_record_pgcs
from uploader.clients.gen.client.adminapi.models.assign_record_pgcs_request import AssignRecordPgcsRequest

ELIGIBLE_QUERY = sql.SQL("""
    SELECT r.id
    FROM layer0.records r
    JOIN layer0.crossmatch c ON c.record_id = r.id
    WHERE r.table_id = %s
      AND r.id > %s
      AND r.pgc IS NULL
      AND c.triage_status = 'resolved'
      AND NOT (c.metadata::jsonb ? 'possible_matches')
    ORDER BY r.id ASC
    LIMIT %s
""")

ELIGIBLE_COUNT_QUERY = """
    SELECT COUNT(*) AS cnt
    FROM layer0.records r
    JOIN layer0.crossmatch c ON c.record_id = r.id
    WHERE r.table_id = %s
      AND r.pgc IS NULL
      AND c.triage_status = 'resolved'
      AND NOT (c.metadata::jsonb ? 'possible_matches')
"""


def run_submit_crossmatch(
    storage: PgStorage,
    table_name: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    report_func: Callable[[report.Event], None],
    *,
    write: bool = False,
) -> None:
    table_rows = storage.query(
        "SELECT id FROM layer0.tables WHERE table_name = %s",
        (table_name,),
    )
    if not table_rows:
        raise RuntimeError(f"Table not found: {table_name}")
    table_id = table_rows[0]["id"]

    eligible_total = int(storage.query(ELIGIBLE_COUNT_QUERY, (table_id,))[0]["cnt"])
    report_func(
        report.LogEvent(
            message=f"Submitting crossmatch for {table_name}: {eligible_total} eligible records (write={write}).",
        )
    )

    submitted = 0
    last_id = ""
    while True:
        rows = storage.query(ELIGIBLE_QUERY, (table_id, last_id, batch_size))
        if not rows:
            break
        record_ids = [r["id"] for r in rows]
        last_id = record_ids[-1]
        if write:
            handle_call(
                assign_record_pgcs.sync_detailed(
                    client=client,
                    body=action_description.apply(
                        AssignRecordPgcsRequest(record_ids=record_ids),
                    ),
                )
            )
        submitted += len(record_ids)
        report_func(
            report.LogEvent(
                message=f"Batch submitted: {len(record_ids)} records ({submitted}/{eligible_total}).",
            )
        )
        progress = 100.0 if eligible_total == 0 else (100.0 * submitted / eligible_total)
        report_func(report.ProgressEvent(percent=min(progress, 100.0)))

    report_func(report.ProgressEvent(percent=100))
    report_func(
        report.DoneEvent(
            message=f"Submitted {submitted}/{eligible_total} records (write={write}).",
        )
    )
