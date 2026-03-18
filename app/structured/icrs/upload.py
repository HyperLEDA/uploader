from collections.abc import Callable
from typing import Any

from psycopg import sql

from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import get_table, save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
from app.lib.rawdata import rawdata_batches
from app.storage import PgStorage
from app.upload import handle_call

ICRS_COLUMNS = ["ra", "dec", "e_ra", "e_dec"]


def _fetch_units(
    client: adminapi.AuthenticatedClient,
    table_name: str,
    ra_column: str,
    dec_column: str,
    ra_error_unit: str,
    dec_error_unit: str,
) -> SaveStructuredDataRequestUnits:
    resp = handle_call(get_table.sync_detailed(client=client, table_name=table_name))
    column_units: dict[str, str] = {}
    for col in resp.data.column_info:
        if isinstance(col.unit, str):
            column_units[col.name] = col.unit
    missing = [c for c in (ra_column, dec_column) if c not in column_units]
    if missing:
        raise RuntimeError(f"Table {table_name} has no unit for column(s): {missing}")
    units_dict = {
        "ra": column_units[ra_column],
        "dec": column_units[dec_column],
        "e_ra": ra_error_unit,
        "e_dec": dec_error_unit,
    }
    return SaveStructuredDataRequestUnits.from_dict(units_dict)


def upload_icrs(
    storage: PgStorage,
    table_name: str,
    ra_column: str,
    dec_column: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    ra_error: float,
    ra_error_unit: str,
    dec_error: float,
    dec_error_unit: str,
    report: Callable[[dict[str, Any]], None] | None = None,
) -> int:
    units = _fetch_units(
        client,
        table_name,
        ra_column,
        dec_column,
        ra_error_unit,
        dec_error_unit,
    )
    uploaded = 0
    skipped = 0
    ra_min = float("inf")
    ra_max = float("-inf")
    dec_min = float("inf")
    dec_max = float("-inf")
    ra_sum = 0.0
    dec_sum = 0.0
    total_count = 0
    if report is not None:
        cnt = storage.query(
            sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
            (),
        )
        total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0

    for rows in rawdata_batches(storage, table_name, [ra_column, dec_column], batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[float]] = []

        for row in rows:
            ra_val = row[ra_column]
            dec_val = row[dec_column]
            if ra_val is None or dec_val is None:
                skipped += 1
                continue
            ra_f = float(ra_val)
            dec_f = float(dec_val)
            batch_ids.append(row["hyperleda_internal_id"])
            batch_data.append([ra_f, dec_f, float(ra_error), float(dec_error)])
            uploaded += 1
            ra_min = min(ra_min, ra_f)
            ra_max = max(ra_max, ra_f)
            dec_min = min(dec_min, dec_f)
            dec_max = max(dec_max, dec_f)
            ra_sum += ra_f
            dec_sum += dec_f

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=SaveStructuredDataRequest(
                        catalog="icrs",
                        columns=ICRS_COLUMNS,
                        ids=batch_ids,
                        data=batch_data,
                        units=units,
                    ),
                )
            )

        processed_rows += len(rows)
        if report is not None:
            row_pct = int(100 * processed_rows / total_count) if total_count else 0
            report({"type": "progress", "percent": min(99, row_pct)})
            report(
                {
                    "type": "log",
                    "message": (f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}"),
                },
            )

    total = uploaded + skipped

    def row_pct_label(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows: list[tuple[str, int | float, float | str]] = [
        ("Uploaded", uploaded, row_pct_label(uploaded)),
        ("Skipped (null)", skipped, row_pct_label(skipped)),
    ]
    if uploaded > 0:
        ra_mean = ra_sum / uploaded
        dec_mean = dec_sum / uploaded
        table_rows.extend(
            [
                ("RA min", round(ra_min, 6), "-"),
                ("RA max", round(ra_max, 6), "-"),
                ("RA mean", round(ra_mean, 6), "-"),
                ("Dec min", round(dec_min, 6), "-"),
                ("Dec max", round(dec_max, 6), "-"),
                ("Dec mean", round(dec_mean, 6), "-"),
            ]
        )
    if report is not None:
        report({"type": "progress", "percent": 100})
        lines = [f"Total rows: {total}", f"{'Status':<20} {'Count':>8} {'%':>6}"]
        for label, c, p in table_rows:
            p_str = f"{p:>5.1f}" if isinstance(p, float) else str(p)
            lines.append(f"{label:<20} {c!s:>8} {p_str:>6}")
        report({"type": "log", "message": "\n".join(lines)})
    else:
        print_table(
            ("Status", "Count", "%"),
            table_rows,
            title=f"Total rows: {total}\n",
        )
    return total
