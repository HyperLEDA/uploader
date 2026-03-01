from psycopg import connect, sql

from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
from app.upload import handle_call

C_KM_S = 299792.458

REDSHIFT_COLUMNS = ["cz", "e_cz"]

REDSHIFT_UNITS = SaveStructuredDataRequestUnits.from_dict({"cz": "km/s", "e_cz": "km/s"})


def upload_redshift(
    dsn: str,
    table_name: str,
    z_column: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    z_error: float,
) -> None:
    id_col = sql.Identifier("hyperleda_internal_id")
    table = sql.SQL("rawdata.") + sql.Identifier(table_name)
    query = sql.SQL("SELECT {id_col}, {z} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s").format(
        id_col=id_col,
        z=sql.Identifier(z_column),
        t=table,
    )
    uploaded = 0
    skipped = 0
    cz_min = float("inf")
    cz_max = float("-inf")
    cz_sum = 0.0

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
                z_val = row[1]
                if z_val is None:
                    skipped += 1
                    continue
                cz_val = float(z_val) * C_KM_S
                e_cz = float(z_error) * C_KM_S
                batch_ids.append(last_id)
                batch_data.append([cz_val, e_cz])
                uploaded += 1
                cz_min = min(cz_min, cz_val)
                cz_max = max(cz_max, cz_val)
                cz_sum += cz_val

            if write and batch_ids:
                handle_call(
                    save_structured_data.sync_detailed(
                        client=client,
                        body=SaveStructuredDataRequest(
                            catalog="redshift",
                            columns=REDSHIFT_COLUMNS,
                            ids=batch_ids,
                            data=batch_data,
                            units=REDSHIFT_UNITS,
                        ),
                    )
                )

    total = uploaded + skipped

    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows: list[tuple[str, int | float, float | str]] = [
        ("Uploaded", uploaded, pct(uploaded)),
        ("Skipped (null)", skipped, pct(skipped)),
    ]
    if uploaded > 0:
        cz_mean = cz_sum / uploaded
        table_rows.extend(
            [
                ("cz min (km/s)", round(cz_min, 2), "-"),
                ("cz max (km/s)", round(cz_max, 2), "-"),
                ("cz mean (km/s)", round(cz_mean, 2), "-"),
            ]
        )
    print_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total rows: {total}\n",
    )
