from collections.abc import Callable

from psycopg import sql

import uploader.app.report as report
from uploader.app import log
from uploader.app.display import format_table
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.storage import PgStorage
from uploader.app.structured.designations.rules import RULES, match
from uploader.clients.client import call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)


def upload_designations(
    storage: PgStorage,
    table_name: str,
    column_name: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    print_unmatched: bool = False,
    initial_offset: int = 0,
    report_func: Callable[[report.Event], None],
) -> int:
    rule_counts: dict[str, int] = {r.name: 0 for r in RULES}
    unmatched = 0
    total_count = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0

    processed_rows = 0
    rows_remaining_to_skip = initial_offset
    progress_total = max(1, total_count - initial_offset)
    skipped_rows_total = 0

    for rows in rawdata_batches(storage, table_name, [column_name], batch_size):
        batch = rows
        if rows_remaining_to_skip:
            n = len(batch)
            if rows_remaining_to_skip >= n:
                skipped_rows_total += n
                rows_remaining_to_skip -= n
                report_func(report.LogEvent(message=f"skipped rows due to offset: {n}"))
                continue
            skipped_rows_total += rows_remaining_to_skip
            batch = batch[rows_remaining_to_skip:]
            rows_remaining_to_skip = 0

        batch_ids: list[str] = []
        batch_names: list[list[str]] = []

        for row in batch:
            internal_id = row["hyperleda_internal_id"]
            name_val = row[column_name]
            if name_val is None or (isinstance(name_val, str) and not name_val.strip()):
                unmatched += 1
                continue
            name_str = str(name_val).strip()
            match_result = match(name_str)
            if match_result is not None:
                transformed, rule_name = match_result
                rule_counts[rule_name] += 1
            else:
                unmatched += 1
                transformed = name_str
                if print_unmatched:
                    report_func(report.LogEvent(message=name_str))
            batch_ids.append(internal_id)
            batch_names.append([transformed])

        if write and batch_ids:
            call(
                client,
                SaveStructuredDataRequest(
                    catalog="designation",
                    columns=["design"],
                    ids=batch_ids,
                    data=batch_names,
                ),
                save_structured_data.sync_detailed,
                callback_func=lambda m: report_func(report.LogEvent(message=m)),
            )

        processed_rows += len(batch)
        total_so_far = sum(rule_counts.values()) + unmatched
        cumulative_names = total_so_far + skipped_rows_total

        def total_pct(n: int, t: int = total_so_far) -> float:
            return (100.0 * n / t) if t else 0.0

        log.logger.info(
            "processed batch",
            total=total_so_far,
            matched=sum(rule_counts.values()),
            matched_pct=round(total_pct(sum(rule_counts.values())), 1),
            unmatched=unmatched,
            unmatched_pct=round(total_pct(unmatched), 1),
        )
        progress_pct = int(100 * processed_rows / progress_total) if total_count else 0
        report_func(report.ProgressEvent(percent=min(99, progress_pct)))
        report_func(
            report.LogEvent(
                message=(
                    f"batch: rows_read={len(batch)} cumulative_names={cumulative_names} "
                    f"matched={sum(rule_counts.values())} unmatched={unmatched}"
                ),
            ),
        )

    total = sum(rule_counts.values()) + unmatched

    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows = [
        (name, rule_counts[name], pct(rule_counts[name]))
        for name in sorted(rule_counts.keys(), key=lambda n: (-rule_counts[n], n))
        if rule_counts[name] > 0
    ]
    table_rows.append(("(no rule matched)", unmatched, pct(unmatched)))

    report_func(report.ProgressEvent(percent=100))
    summary = format_table(
        ("Rule", "Count", "%"),
        table_rows,
        title=f"Total names: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))

    return total
