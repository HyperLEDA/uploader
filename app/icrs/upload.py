from psycopg import sql

from app import log
from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import get_table, save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
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
) -> None:
    units = _fetch_units(
        client,
        table_name,
        ra_column,
        dec_column,
        ra_error_unit,
        dec_error_unit,
    )
    id_col = sql.Identifier("hyperleda_internal_id")
    table = sql.SQL("rawdata.") + sql.Identifier(table_name)
    query = sql.SQL("SELECT {id_col}, {ra}, {dec} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s").format(
        id_col=id_col,
        ra=sql.Identifier(ra_column),
        dec=sql.Identifier(dec_column),
        t=table,
    )
    uploaded = 0
    skipped = 0
    ra_min = float("inf")
    ra_max = float("-inf")
    dec_min = float("inf")
    dec_max = float("-inf")
    ra_sum = 0.0
    dec_sum = 0.0

    last_id = ""
    while True:
        rows = storage.query(query, (last_id, batch_size))
        if not rows:
            break

        batch_ids: list[str] = []
        batch_data: list[list[float]] = []

        for row in rows:
            last_id = row["hyperleda_internal_id"]
            ra_val = row[ra_column]
            dec_val = row[dec_column]
            if ra_val is None or dec_val is None:
                skipped += 1
                continue
            ra_f = float(ra_val)
            dec_f = float(dec_val)
            batch_ids.append(last_id)
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

        log.logger.debug(
            "processed batch",
            rows=len(rows),
            last_id=last_id,
            total=uploaded,
        )

    total = uploaded + skipped

    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows: list[tuple[str, int | float, float | str]] = [
        ("Uploaded", uploaded, pct(uploaded)),
        ("Skipped (null)", skipped, pct(skipped)),
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
    print_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total rows: {total}\n",
    )
