from collections import Counter
from collections.abc import Callable

import matplotlib.pyplot as plt
from psycopg import sql

import uploader.app.report as report
from uploader.app.display import format_table
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.storage import PgStorage
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)

NATURE_COLUMNS = ["type_name"]

CHART_FIGSIZE = (8, 6)


def _type_distribution_bars(type_counts: Counter[str]) -> list[tuple[str, int]]:
    sorted_types = sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    top = [(name, count) for name, count in sorted_types[:10] if count > 0]
    other_total = sum(count for _, count in sorted_types[10:])
    bars: list[tuple[str, int]] = list(top)
    if other_total > 0:
        bars.append(("(other types)", other_total))
    return bars


def _emit_type_distribution_image(
    report_func: Callable[[report.Event], None],
    type_counts: Counter[str],
    *,
    caption: str,
) -> None:
    bars = _type_distribution_bars(type_counts)
    if not bars:
        return
    labels = [name for name, _ in bars]
    counts = [count for _, count in bars]
    fig, ax = plt.subplots(figsize=CHART_FIGSIZE)
    ax.barh(labels, counts)
    ax.invert_yaxis()
    ax.set_xlabel("Count")
    ax.set_title("LEDA type distribution")
    report_func(report.image_event_from_figure(fig, caption=caption))


def upload_nature(
    storage: PgStorage,
    table_name: str,
    column_name: str | None,
    type_mapping: dict[str, str],
    default_type: str | None,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    report_func: Callable[[report.Event], None],
) -> int:
    total_uploaded = 0
    type_counts: Counter[str] = Counter()

    columns: list[str] = [] if column_name is None else [column_name]
    total_count = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0

    for rows in rawdata_batches(storage, table_name, columns, batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[str]] = []

        for row in rows:
            leda_type: str | None = default_type
            if column_name is not None:
                raw_val = row[column_name]
                raw_key = str(raw_val).strip() if raw_val is not None else ""
                leda_type = type_mapping.get(raw_key, default_type if default_type is not None else raw_key)

            if leda_type is None:
                raise RuntimeError("leda_type is None: set --default or ensure type_mapping covers all values")
            batch_ids.append(row["hyperleda_internal_id"])
            batch_data.append([leda_type])
            type_counts[leda_type] += 1
            total_uploaded += 1

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=SaveStructuredDataRequest(
                        catalog="nature",
                        columns=NATURE_COLUMNS,
                        ids=batch_ids,
                        data=batch_data,
                    ),
                )
            )

        processed_rows += len(rows)
        batch_pct = int(100 * processed_rows / total_count) if total_count else 0
        report_func(report.ProgressEvent(percent=min(99, batch_pct)))
        report_func(
            report.LogEvent(
                message=f"batch: rows_read={len(rows)} total_uploaded_so_far={total_uploaded}",
            ),
        )
        _emit_type_distribution_image(
            report_func,
            type_counts,
            caption=f"{total_uploaded} rows classified",
        )

    table_rows: list[tuple[str, int, float | str]] = [
        (
            leda_type,
            count,
            (100.0 * count / total_uploaded) if total_uploaded else "-",
        )
        for leda_type, count in sorted(type_counts.items())
    ]
    report_func(report.ProgressEvent(percent=100))
    _emit_type_distribution_image(
        report_func,
        type_counts,
        caption=f"Final: {total_uploaded} rows",
    )
    summary = format_table(
        ("LEDA type", "Count", "%"),
        table_rows,
        title=f"Total rows: {total_uploaded}\n",
    )
    report_func(report.DoneEvent(message=summary))
    return total_uploaded
