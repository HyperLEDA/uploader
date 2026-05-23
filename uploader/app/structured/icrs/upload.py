from collections.abc import Callable

import matplotlib.pyplot as plt
import numpy as np
from psycopg import sql

import uploader.app.report as report
from uploader.app.display import format_table
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.storage import PgStorage
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import get_table, save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from uploader.clients.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)

ICRS_COLUMNS = ["ra", "dec", "e_ra", "e_dec"]

CHART_FIGSIZE = (10, 5)
N_RA_BINS = 72
N_DEC_BINS = 36
RA_BIN_EDGES = np.linspace(-180.0, 180.0, N_RA_BINS + 1)
DEC_BIN_EDGES = np.linspace(-90.0, 90.0, N_DEC_BINS + 1)


def _ra_to_longitude_deg(ra: np.ndarray) -> np.ndarray:
    return (np.mod(ra, 360.0) + 180.0) % 360.0 - 180.0


class _SkyCoverageAccumulator:
    def __init__(self) -> None:
        self._counts = np.zeros((N_RA_BINS, N_DEC_BINS), dtype=np.int64)

    def add(self, ra: list[float], dec: list[float]) -> None:
        if not ra:
            return
        ra_arr = _ra_to_longitude_deg(np.asarray(ra, dtype=np.float64))
        dec_arr = np.clip(np.asarray(dec, dtype=np.float64), -90.0, 90.0)
        batch_counts, _, _ = np.histogram2d(ra_arr, dec_arr, bins=[RA_BIN_EDGES, DEC_BIN_EDGES])
        self._counts += batch_counts.astype(np.int64)

    @property
    def total(self) -> int:
        return int(self._counts.sum())

    def emit_image(
        self,
        report_func: Callable[[report.Event], None],
        *,
        caption: str,
    ) -> None:
        if self.total == 0:
            return
        ra_centers = 0.5 * (RA_BIN_EDGES[:-1] + RA_BIN_EDGES[1:])
        dec_centers = 0.5 * (DEC_BIN_EDGES[:-1] + DEC_BIN_EDGES[1:])
        theta, phi = np.meshgrid(-np.deg2rad(ra_centers), np.deg2rad(dec_centers))
        fig = plt.figure(figsize=CHART_FIGSIZE)
        ax = fig.add_subplot(111, projection="aitoff")
        ax.pcolormesh(theta, phi, self._counts.T, shading="auto", cmap="viridis")
        ax.set_title("ICRS sky coverage")
        ax.grid(True)
        report_func(report.image_event_from_figure(fig, caption=caption))


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
    report_func: Callable[[report.Event], None],
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
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0
    sky = _SkyCoverageAccumulator()

    for rows in rawdata_batches(storage, table_name, [ra_column, dec_column], batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[float]] = []
        batch_ra: list[float] = []
        batch_dec: list[float] = []

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
            batch_ra.append(ra_f)
            batch_dec.append(dec_f)

        sky.add(batch_ra, batch_dec)

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
        row_pct = int(100 * processed_rows / total_count) if total_count else 0
        report_func(report.ProgressEvent(percent=min(99, row_pct)))
        report_func(
            report.LogEvent(
                message=f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}",
            ),
        )
        sky.emit_image(report_func, caption=f"Sky coverage: {uploaded} objects")

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
    report_func(report.ProgressEvent(percent=100))
    sky.emit_image(report_func, caption=f"Final: {uploaded} objects")
    summary = format_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total rows: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))
    return total
