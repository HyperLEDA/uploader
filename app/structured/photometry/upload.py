import click

from app import log
from app.display import format_table
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

PHOTOMETRY_COLUMNS = ["band", "mag", "e_mag", "method"]

PHOTOMETRY_UNITS = SaveStructuredDataRequestUnits.from_dict({"mag": "mag", "e_mag": "mag"})

BANDS = [
    ("U", "ut", "e_ut"),
    ("B", "bt", "e_bt"),
    ("V", "vt", "e_vt"),
    ("I", "it", "e_it"),
    ("K", "kt", "e_kt"),
]

PHOTOMETRY_RAW_COLUMNS = [c for _, mag, err in BANDS for c in (mag, err)]


def upload_photometry_hyperleda(
    storage: PgStorage,
    table_name: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
) -> None:
    uploaded_objects = 0
    skipped = 0
    total_source_rows = 0
    band_counts: dict[str, int] = {band: 0 for band, _, _ in BANDS}
    band_mag_sums: dict[str, float] = {band: 0.0 for band, _, _ in BANDS}

    try:
        for rows in rawdata_batches(storage, table_name, PHOTOMETRY_RAW_COLUMNS, batch_size):
            total_source_rows += len(rows)
            batch_ids: list[str] = []
            batch_data: list[list[str | float]] = []

            for row in rows:
                internal_id = row["hyperleda_internal_id"]
                had_any = False
                for band, mag_col, err_col in BANDS:
                    mag_val = row.get(mag_col)
                    err_val = row.get(err_col)
                    if mag_val is not None and err_val is not None:
                        batch_ids.append(internal_id)
                        batch_data.append([band, float(mag_val), float(err_val), "asymptotic"])
                        band_counts[band] += 1
                        band_mag_sums[band] += float(mag_val)
                        had_any = True
                if had_any:
                    uploaded_objects += 1
                else:
                    skipped += 1

            if write and batch_ids:
                handle_call(
                    save_structured_data.sync_detailed(
                        client=client,
                        body=SaveStructuredDataRequest(
                            catalog="photometry",
                            columns=PHOTOMETRY_COLUMNS,
                            ids=batch_ids,
                            data=batch_data,
                            units=PHOTOMETRY_UNITS,
                        ),
                    )
                )

            uploaded_rows = sum(band_counts.values())
            log.logger.info(
                "processed batch",
                source_rows=len(rows),
                total_source_rows=total_source_rows,
                objects=uploaded_objects,
                photometry_rows=uploaded_rows,
            )
    finally:
        total = uploaded_objects + skipped
        total_photometry_rows = sum(band_counts.values())

        def pct(n: int, denom: int) -> float:
            return (100.0 * n / denom) if denom else 0.0

        table_rows: list[tuple[str | int | float, ...]] = [
            ("Source rows with ≥1 band", uploaded_objects, f"{pct(uploaded_objects, total):.1f}%", "-"),
            ("Source rows with no band", skipped, f"{pct(skipped, total):.1f}%", "-"),
            ("Total photometry rows", total_photometry_rows, "-", "-"),
        ]
        for band, _, _ in BANDS:
            count = band_counts[band]
            avg_mag = (band_mag_sums[band] / count) if count else 0.0
            pct_str = f"{pct(count, total_photometry_rows):.1f}%" if total_photometry_rows else "-"
            avg_str = round(avg_mag, 3) if count else "-"
            table_rows.append((band, count, pct_str, avg_str))

        click.echo(
            format_table(
                ("Status", "Uploaded", "% of total", "Avg mag"),
                table_rows,
                title=f"Total source rows: {total}\n",
                percent_last_column=False,
            )
        )
