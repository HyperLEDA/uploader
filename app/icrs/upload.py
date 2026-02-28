from typing import cast

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
    e_ra_column: str | None,
    e_dec_column: str | None,
    ra_error_unit: str | None,
    dec_error_unit: str | None,
) -> SaveStructuredDataRequestUnits:
    resp = handle_call(get_table.sync_detailed(client=client, table_name=table_name))
    column_units: dict[str, str] = {}
    for col in resp.data.column_info:
        if isinstance(col.unit, str):
            column_units[col.name] = col.unit
    if e_ra_column is not None and e_dec_column is not None:
        source_to_catalog = {
            ra_column: "ra",
            dec_column: "dec",
            e_ra_column: "e_ra",
            e_dec_column: "e_dec",
        }
        missing = [c for c in (ra_column, dec_column, e_ra_column, e_dec_column) if c not in column_units]
        if missing:
            raise RuntimeError(f"Table {table_name} has no unit for column(s): {missing}")
        units_dict = {catalog: column_units[source] for source, catalog in source_to_catalog.items()}
    else:
        missing = [c for c in (ra_column, dec_column) if c not in column_units]
        if missing:
            raise RuntimeError(f"Table {table_name} has no unit for column(s): {missing}")
        if ra_error_unit is None or dec_error_unit is None:
            raise RuntimeError("ra_error_unit and dec_error_unit are required when not using error columns")
        units_dict = {
            "ra": column_units[ra_column],
            "dec": column_units[dec_column],
            "e_ra": cast(str, ra_error_unit),
            "e_dec": cast(str, dec_error_unit),
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
    e_ra_column: str | None = None,
    e_dec_column: str | None = None,
    ra_error: float | None = None,
    ra_error_unit: str | None = None,
    dec_error: float | None = None,
    dec_error_unit: str | None = None,
) -> None:
    use_fixed_errors = e_ra_column is None and e_dec_column is None
    if use_fixed_errors:
        assert ra_error is not None and ra_error_unit is not None
        assert dec_error is not None and dec_error_unit is not None
    units = _fetch_units(
        client,
        table_name,
        ra_column,
        dec_column,
        e_ra_column,
        e_dec_column,
        ra_error_unit,
        dec_error_unit,
    )
    id_col = sql.Identifier("hyperleda_internal_id")
    table = sql.SQL("rawdata.") + sql.Identifier(table_name)
    if use_fixed_errors:
        cols = [sql.Identifier(ra_column), sql.Identifier(dec_column)]
        query = sql.SQL(
            "SELECT {id_col}, {ra}, {dec} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s"
        ).format(id_col=id_col, ra=cols[0], dec=cols[1], t=table)
    else:
        e_ra_col = e_ra_column
        e_dec_col = e_dec_column
        assert e_ra_col is not None and e_dec_col is not None
        cols = [
            sql.Identifier(ra_column),
            sql.Identifier(dec_column),
            sql.Identifier(e_ra_col),
            sql.Identifier(e_dec_col),
        ]
        query = sql.SQL(
            "SELECT {id_col}, {ra}, {dec}, {e_ra}, {e_dec} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s"
        ).format(
            id_col=id_col,
            ra=cols[0],
            dec=cols[1],
            e_ra=cols[2],
            e_dec=cols[3],
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
                if use_fixed_errors:
                    e_ra_val = ra_error
                    e_dec_val = dec_error
                else:
                    e_ra_val = row[3]
                    e_dec_val = row[4]
                if ra_val is None or dec_val is None or e_ra_val is None or e_dec_val is None:
                    skipped += 1
                    continue
                batch_ids.append(last_id)
                batch_data.append([float(ra_val), float(dec_val), float(e_ra_val), float(e_dec_val)])
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
