from collections.abc import Callable, Mapping, Sequence
from typing import Any

from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app.display import format_table
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.storage import PgStorage
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.datatype_enum import DatatypeEnum
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from uploader.clients.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
from uploader.clients.gen.client.adminapi.types import UNSET

_FLOAT_TYPES = {
    DatatypeEnum.DOUBLE,
    DatatypeEnum.DOUBLE_PRECISION,
    DatatypeEnum.FLOAT,
    DatatypeEnum.NUMBER,
    DatatypeEnum.REAL,
}
_INT_TYPES = {
    DatatypeEnum.INT,
    DatatypeEnum.INTEGER,
    DatatypeEnum.LONG,
    DatatypeEnum.POSITIVEINTEGER,
    DatatypeEnum.SHORT,
    DatatypeEnum.SMALLINT,
    DatatypeEnum.UNSIGNEDBYTE,
    DatatypeEnum.UNSIGNEDINT,
    DatatypeEnum.UNSIGNEDLONG,
    DatatypeEnum.UNSIGNEDSHORT,
}


def _coerce_value(value: Any, data_type: DatatypeEnum) -> Any:
    if data_type in _FLOAT_TYPES:
        return float(value)
    if data_type in _INT_TYPES:
        return int(value)
    return str(value)


def upload_catalog_columns(
    storage: PgStorage,
    table_name: str,
    catalog: str,
    column_map: Mapping[str, str],
    field_types: Mapping[str, DatatypeEnum],
    field_units: Mapping[str, str],
    columns: Sequence[str],
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    report_func: Callable[[report.Event], None],
) -> int:
    if not columns:
        raise RuntimeError("No catalog columns selected for upload")

    source_columns = [column_map[name] for name in columns]
    units_payload: dict[str, str] = {name: field_units[name] for name in columns if name in field_units}
    units = SaveStructuredDataRequestUnits.from_dict(units_payload) if units_payload else UNSET

    uploaded = 0
    skipped = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0

    for rows in rawdata_batches(storage, table_name, source_columns, batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[Any]] = []

        for row in rows:
            if any(row[src] is None for src in source_columns):
                skipped += 1
                continue
            values = [_coerce_value(row[column_map[name]], field_types[name]) for name in columns]
            batch_ids.append(row["hyperleda_internal_id"])
            batch_data.append(values)
            uploaded += 1

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=action_description.apply(
                        SaveStructuredDataRequest(
                            catalog=catalog,
                            columns=list(columns),
                            ids=batch_ids,
                            data=batch_data,
                            units=units,
                        ),
                    ),
                )
            )

        processed_rows += len(rows)
        row_pct = int(100 * processed_rows / total_count) if total_count else 0
        report_func(report.ProgressEvent(percent=min(99, row_pct)))
        report_func(
            report.LogEvent(
                message=f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}",
            ),
        )

    total = uploaded + skipped

    def row_pct_label(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    report_func(report.ProgressEvent(percent=100))
    summary = format_table(
        ("Status", "Count", "%"),
        [
            ("Uploaded", uploaded, row_pct_label(uploaded)),
            ("Skipped (null)", skipped, row_pct_label(skipped)),
        ],
        title=f"Total rows: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))
    return total
