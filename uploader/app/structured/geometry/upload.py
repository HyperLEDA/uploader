from collections.abc import Callable

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
from psycopg import sql

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

BASE_GEOMETRY_COLUMNS = ["band", "method", "isophote", "a", "e_a", "b", "e_b"]
OPTIONAL_GEOMETRY_COLUMNS = ["pa", "e_pa"]

TARGET_UNITS = {
    "a": "arcsec",
    "e_a": "arcsec",
    "b": "arcsec",
    "e_b": "arcsec",
    "pa": "deg",
    "e_pa": "deg",
    "isophote": "mag/arcmin2",
}

CHART_FIGSIZE = (8, 6)
N_A_BINS = 80
A_BIN_MIN = 0.05
A_BIN_MAX = 500.0
A_BIN_EDGES = np.logspace(np.log10(A_BIN_MIN), np.log10(A_BIN_MAX), N_A_BINS + 1)


class _SemiMajorAxisAccumulator:
    def __init__(self) -> None:
        self._counts = np.zeros(N_A_BINS, dtype=np.int64)

    def add(self, a_values: list[float]) -> None:
        if not a_values:
            return
        positive = np.asarray([v for v in a_values if v > 0], dtype=np.float64)
        if positive.size == 0:
            return
        batch_counts, _ = np.histogram(positive, bins=A_BIN_EDGES)
        self._counts += batch_counts.astype(np.int64)

    @property
    def total(self) -> int:
        return int(self._counts.sum())

    def emit_image(
        self,
        report_func: Callable[[report.Event], None],
        *,
        caption: str,
        a_mean: float | None = None,
        a_min: float | None = None,
        a_max: float | None = None,
    ) -> None:
        if self.total == 0:
            return
        centers = np.sqrt(A_BIN_EDGES[:-1] * A_BIN_EDGES[1:])
        widths = np.diff(A_BIN_EDGES)
        fig, ax = plt.subplots(figsize=CHART_FIGSIZE)
        ax.bar(centers, self._counts, width=widths, align="center")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("a (arcsec)")
        ax.set_ylabel("Count")
        ax.set_title("Semi-major axis distribution")
        if a_min is not None and a_min > 0:
            ax.axvline(a_min, color="gray", linestyle=":", linewidth=1)
        if a_max is not None and a_max > 0:
            ax.axvline(a_max, color="gray", linestyle=":", linewidth=1)
        if a_mean is not None and a_mean > 0:
            ax.axvline(a_mean, color="red", linestyle="--", linewidth=1, label=f"mean={a_mean:.2f}")
            ax.legend(loc="upper right", fontsize="small")
        report_func(report.image_event_from_figure(fig, caption=caption))


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


def _parse_expressions(expressions: dict[str, str]) -> dict[str, Expression]:
    return {field: parse(source) for field, source in expressions.items()}


def _validate_columns(
    table_name: str,
    needed_cols: set[str],
    column_names: set[str],
) -> None:
    missing = sorted(col for col in needed_cols if col not in column_names)
    if missing:
        raise RuntimeError(f"Table {table_name} has no column(s): {missing}")


def _evaluate_field(
    expr: Expression,
    values: dict[str, float],
    column_units: dict[str, str],
    field: str,
) -> float:
    quantity = expr.evaluate(values, column_units).to(u.Unit(TARGET_UNITS[field]))
    return float(quantity.value)


def upload_geometry_isophotal(
    storage: PgStorage,
    table_name: str,
    band: str,
    expressions: dict[str, str],
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    report_func: Callable[[report.Event], None],
) -> int:
    parsed = _parse_expressions(expressions)
    geometry_columns = BASE_GEOMETRY_COLUMNS + [col for col in OPTIONAL_GEOMETRY_COLUMNS if col in parsed]
    geometry_units = SaveStructuredDataRequestUnits.from_dict(
        {col: TARGET_UNITS[col] for col in geometry_columns if col in TARGET_UNITS},
    )
    needed_cols = set().union(*(expr.referenced_columns for expr in parsed.values()))
    column_names, column_units = _fetch_column_units(client, table_name)
    _validate_columns(table_name, needed_cols, column_names)

    uploaded = 0
    skipped = 0
    a_min = float("inf")
    a_max = float("-inf")
    a_sum = 0.0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0
    a_dist = _SemiMajorAxisAccumulator()

    for rows in rawdata_batches(storage, table_name, sorted(needed_cols), batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[str | float]] = []
        batch_a: list[float] = []

        for row in rows:
            if any(row[col] is None for col in needed_cols):
                skipped += 1
                continue

            values = {col: float(row[col]) for col in needed_cols}
            try:
                evaluated = {
                    field: _evaluate_field(expr, values, column_units, field) for field, expr in parsed.items()
                }
            except (ValueError, u.UnitConversionError, u.UnitTypeError) as e:
                raise RuntimeError(
                    f"failed to evaluate expressions for row {row['hyperleda_internal_id']}: {e}",
                ) from e

            row_data: dict[str, str | float | None] = {
                "band": band,
                "method": "isophotal",
                "isophote": evaluated["isophote"],
                "a": evaluated["a"],
                "e_a": evaluated["e_a"],
                "b": evaluated["b"],
                "e_b": evaluated["e_b"],
            }
            for col in OPTIONAL_GEOMETRY_COLUMNS:
                if col in parsed:
                    row_data[col] = evaluated[col]

            batch_ids.append(row["hyperleda_internal_id"])
            batch_data.append([row_data[col] for col in geometry_columns])
            uploaded += 1
            a_val = evaluated["a"]
            a_min = min(a_min, a_val)
            a_max = max(a_max, a_val)
            a_sum += a_val
            batch_a.append(a_val)

        a_dist.add(batch_a)

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=SaveStructuredDataRequest(
                        catalog="geometry",
                        columns=geometry_columns,
                        ids=batch_ids,
                        data=batch_data,
                        units=geometry_units,
                    ),
                ),
            )

        processed_rows += len(rows)
        row_pct = int(100 * processed_rows / total_count) if total_count else 0
        report_func(report.ProgressEvent(percent=min(99, row_pct)))
        report_func(
            report.LogEvent(
                message=f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}",
            ),
        )
        if uploaded > 0:
            a_dist.emit_image(
                report_func,
                caption=f"a distribution: {uploaded} objects",
                a_mean=a_sum / uploaded,
                a_min=a_min,
                a_max=a_max,
            )

    total = uploaded + skipped

    def row_pct_label(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows: list[tuple[str, int | float, float | str]] = [
        ("Uploaded", uploaded, row_pct_label(uploaded)),
        ("Skipped (null)", skipped, row_pct_label(skipped)),
    ]
    if uploaded > 0:
        a_mean = a_sum / uploaded
        table_rows.extend(
            [
                ("a min (arcsec)", round(a_min, 3), "-"),
                ("a max (arcsec)", round(a_max, 3), "-"),
                ("a mean (arcsec)", round(a_mean, 3), "-"),
            ],
        )
    report_func(report.ProgressEvent(percent=100))
    if uploaded > 0:
        a_dist.emit_image(
            report_func,
            caption=f"Final: {uploaded} objects",
            a_mean=a_sum / uploaded,
            a_min=a_min,
            a_max=a_max,
        )
    summary = format_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total rows: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))
    return total
