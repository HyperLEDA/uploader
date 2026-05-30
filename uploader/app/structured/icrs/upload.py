from collections.abc import Callable

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app.display import format_table
from uploader.app.lib.expression import Expression, parse
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


TARGET_ERROR_UNITS = {
    "e_ra": "arcsec",
    "e_dec": "arcsec",
}


def _parse_expressions(expressions: dict[str, str]) -> dict[str, Expression]:
    return {field: parse(source) for field, source in expressions.items()}


def _evaluate_error_field(
    expr: Expression,
    values: dict[str, float],
    column_units: dict[str, str],
    field: str,
) -> float:
    quantity = expr.evaluate(values, column_units).to(u.Unit(TARGET_ERROR_UNITS[field]))
    return float(quantity.value)


def _fetch_column_units(
    client: adminapi.AuthenticatedClient,
    table_name: str,
) -> tuple[set[str], dict[str, str]]:
    resp = handle_call(get_table.sync_detailed(client=client, table_name=table_name))
    column_names: set[str] = set()
    column_units: dict[str, str] = {}
    for col in resp.data.column_info:
        column_names.add(col.name)
        if isinstance(col.unit, str):
            column_units[col.name] = col.unit
    return column_names, column_units


def upload_icrs(
    storage: PgStorage,
    table_name: str,
    ra_column: str,
    dec_column: str,
    expressions: dict[str, str],
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    report_func: Callable[[report.Event], None],
) -> int:
    parsed = _parse_expressions(expressions)
    column_names, column_units = _fetch_column_units(client, table_name)

    error_cols = set().union(*(expr.referenced_columns for expr in parsed.values()))
    all_needed_cols = {ra_column, dec_column} | error_cols
    missing = sorted(col for col in all_needed_cols if col not in column_names)
    if missing:
        raise RuntimeError(f"Table {table_name} has no column(s): {missing}")

    missing_units = [c for c in (ra_column, dec_column) if c not in column_units]
    if missing_units:
        raise RuntimeError(f"Table {table_name} has no unit for column(s): {missing_units}")

    units = SaveStructuredDataRequestUnits.from_dict(
        {
            "ra": column_units[ra_column],
            "dec": column_units[dec_column],
            **TARGET_ERROR_UNITS,
        }
    )

    uploaded = 0
    skipped = 0
    ra_min = float("inf")
    ra_max = float("-inf")
    dec_min = float("inf")
    dec_max = float("-inf")
    ra_sum = 0.0
    dec_sum = 0.0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0
    sky = _SkyCoverageAccumulator()

    fetch_columns = sorted(all_needed_cols)
    for rows in rawdata_batches(storage, table_name, fetch_columns, batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[float]] = []
        batch_ra: list[float] = []
        batch_dec: list[float] = []

        for row in rows:
            if any(row[col] is None for col in all_needed_cols):
                skipped += 1
                continue

            ra_f = float(row[ra_column])
            dec_f = float(row[dec_column])

            values = {col: float(row[col]) for col in error_cols}
            try:
                e_ra_val = _evaluate_error_field(parsed["e_ra"], values, column_units, "e_ra")
                e_dec_val = _evaluate_error_field(parsed["e_dec"], values, column_units, "e_dec")
            except (ValueError, u.UnitConversionError, u.UnitTypeError) as e:
                raise RuntimeError(
                    f"failed to evaluate expressions for row {row['hyperleda_internal_id']}: {e}",
                ) from e

            batch_ids.append(row["hyperleda_internal_id"])
            batch_data.append([ra_f, dec_f, e_ra_val, e_dec_val])
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
                    body=action_description.apply(
                        SaveStructuredDataRequest(
                            catalog="icrs",
                            columns=ICRS_COLUMNS,
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
