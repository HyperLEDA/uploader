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
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from uploader.clients.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)

C_KM_S = 299792.458

REDSHIFT_COLUMNS = ["cz", "e_cz"]

REDSHIFT_UNITS = SaveStructuredDataRequestUnits.from_dict({"cz": "km/s", "e_cz": "km/s"})

CHART_FIGSIZE = (8, 6)
N_CZ_BINS = 80
CZ_BIN_MIN = -10_000.0
CZ_BIN_MAX = 350_000.0
CZ_BIN_EDGES = np.linspace(CZ_BIN_MIN, CZ_BIN_MAX, N_CZ_BINS + 1)


class _CzDistributionAccumulator:
    def __init__(self) -> None:
        self._counts = np.zeros(N_CZ_BINS, dtype=np.int64)

    def add(self, cz_values: list[float]) -> None:
        if not cz_values:
            return
        batch_counts, _ = np.histogram(np.asarray(cz_values, dtype=np.float64), bins=CZ_BIN_EDGES)
        self._counts += batch_counts.astype(np.int64)

    @property
    def total(self) -> int:
        return int(self._counts.sum())

    def emit_image(
        self,
        report_func: Callable[[report.Event], None],
        *,
        caption: str,
        cz_mean: float | None = None,
        cz_min: float | None = None,
        cz_max: float | None = None,
    ) -> None:
        if self.total == 0:
            return
        centers = 0.5 * (CZ_BIN_EDGES[:-1] + CZ_BIN_EDGES[1:])
        widths = np.diff(CZ_BIN_EDGES)
        fig, ax = plt.subplots(figsize=CHART_FIGSIZE)
        ax.bar(centers, self._counts, width=widths, align="center")
        ax.set_yscale("log")
        ax.set_xlabel("cz (km/s)")
        ax.set_ylabel("Count")
        ax.set_title("Redshift (cz) distribution")
        if cz_min is not None:
            ax.axvline(cz_min, color="gray", linestyle=":", linewidth=1)
        if cz_max is not None:
            ax.axvline(cz_max, color="gray", linestyle=":", linewidth=1)
        if cz_mean is not None:
            ax.axvline(cz_mean, color="red", linestyle="--", linewidth=1, label=f"mean={cz_mean:.0f}")
            ax.legend(loc="upper right", fontsize="small")
        report_func(report.image_event_from_figure(fig, caption=caption))


def upload_redshift(
    storage: PgStorage,
    table_name: str,
    z_column: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    z_error: float,
    report_func: Callable[[report.Event], None],
) -> int:
    uploaded = 0
    skipped = 0
    cz_min = float("inf")
    cz_max = float("-inf")
    cz_sum = 0.0
    total_count = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0
    cz_dist = _CzDistributionAccumulator()

    for rows in rawdata_batches(storage, table_name, [z_column], batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[float]] = []
        batch_cz: list[float] = []

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
            batch_cz.append(cz_val)

        cz_dist.add(batch_cz)

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
        batch_pct = int(100 * processed_rows / total_count) if total_count else 0
        report_func(report.ProgressEvent(percent=min(99, batch_pct)))
        report_func(
            report.LogEvent(
                message=f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}",
            ),
        )
        if uploaded > 0:
            cz_dist.emit_image(
                report_func,
                caption=f"cz distribution: {uploaded} rows",
                cz_mean=cz_sum / uploaded,
                cz_min=cz_min,
                cz_max=cz_max,
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
    report_func(report.ProgressEvent(percent=100))
    if uploaded > 0:
        cz_dist.emit_image(
            report_func,
            caption=f"Final: {uploaded} rows",
            cz_mean=cz_sum / uploaded,
            cz_min=cz_min,
            cz_max=cz_max,
        )
    summary = format_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total rows: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))
    return total
