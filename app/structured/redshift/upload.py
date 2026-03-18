from collections.abc import Callable
from typing import Any

from psycopg import sql

from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
from app.lib.rawdata import rawdata_batches
from app.storage import PgStorage
from app.upload import handle_call

C_KM_S = 299792.458

REDSHIFT_COLUMNS = ["cz", "e_cz"]

REDSHIFT_UNITS = SaveStructuredDataRequestUnits.from_dict({"cz": "km/s", "e_cz": "km/s"})


def upload_redshift(
    storage: PgStorage,
    table_name: str,
    z_column: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    z_error: float,
    report: Callable[[dict[str, Any]], None] | None = None,
) -> int:
    uploaded = 0
    skipped = 0
    cz_min = float("inf")
    cz_max = float("-inf")
    cz_sum = 0.0
    total_count = 0
    if report is not None:
        cnt = storage.query(
            sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
            (),
        )
        total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0

    for rows in rawdata_batches(storage, table_name, [z_column], batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[float]] = []

        for row in rows:
            z_val = row[z_column]
            if z_val is None:
                skipped += 1
                continue
            cz_val = float(z_val) * C_KM_S
            e_cz = float(z_error) * C_KM_S
            batch_ids.append(row["hyperleda_internal_id"])
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

        processed_rows += len(rows)
        if report is not None:
            batch_pct = int(100 * processed_rows / total_count) if total_count else 0
            report({"type": "progress", "percent": min(99, batch_pct)})
            report(
                {
                    "type": "log",
                    "message": f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}",
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
        cz_mean = cz_sum / uploaded
        table_rows.extend(
            [
                ("cz min (km/s)", round(cz_min, 2), "-"),
                ("cz max (km/s)", round(cz_max, 2), "-"),
                ("cz mean (km/s)", round(cz_mean, 2), "-"),
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
