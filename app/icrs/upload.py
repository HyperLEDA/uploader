from psycopg import connect, sql

from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import get_table, save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
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
    dsn: str,
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

    with connect(dsn) as conn:
        last_id = ""
        while True:
            with conn.cursor() as cur:
                cur.execute(query, (last_id, batch_size))
                rows = cur.fetchall()
            if not rows:
                break

            batch_ids: list[str] = []
            batch_data: list[list[float]] = []

            for row in rows:
                last_id = row[0]
                ra_val = row[1]
                dec_val = row[2]
                if ra_val is None or dec_val is None:
                    skipped += 1
                    continue
                batch_ids.append(last_id)
                batch_data.append([float(ra_val), float(dec_val), float(ra_error), float(dec_error)])
                uploaded += 1

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

    total = uploaded + skipped

    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows = [
        ("Uploaded", uploaded, pct(uploaded)),
        ("Skipped (null)", skipped, pct(skipped)),
    ]
    print_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total rows: {total}\n",
    )
