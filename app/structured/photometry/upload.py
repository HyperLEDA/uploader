from app import log
from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.lib.rawdata import rawdata_batches
from app.storage import PgStorage
from app.upload import handle_call

PHOTOMETRY_COLUMNS = ["band", "mag", "e_mag", "method"]

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
    uploaded_rows = 0
    uploaded_objects = 0
    skipped = 0

    for rows in rawdata_batches(storage, table_name, PHOTOMETRY_RAW_COLUMNS, batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[str | float]] = []

        for row in rows:
            internal_id = row["hyperleda_internal_id"]
            mag_vals = [row[mag_col] for _, mag_col, _ in BANDS]
            err_vals = [row[err_col] for _, _, err_col in BANDS]
            if any(m is None for m in mag_vals) or any(e is None for e in err_vals):
                skipped += 1
                continue
            for (band, _, _), mag_val, err_val in zip(BANDS, mag_vals, err_vals, strict=True):
                batch_ids.append(internal_id)
                batch_data.append([band, float(mag_val), float(err_val), "asymptotic"])
            uploaded_objects += 1
            uploaded_rows += len(BANDS)

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=SaveStructuredDataRequest(
                        catalog="photometry",
                        columns=PHOTOMETRY_COLUMNS,
                        ids=batch_ids,
                        data=batch_data,
                    ),
                )
            )

        log.logger.info(
            "processed batch",
            objects=uploaded_objects,
            photometry_rows=uploaded_rows,
        )

    total = uploaded_objects + skipped

    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows: list[tuple[str, int, float | str]] = [
        ("Uploaded (objects)", uploaded_objects, pct(uploaded_objects)),
        ("Uploaded (photometry rows)", uploaded_rows, "-"),
        ("Skipped (null mag/error)", skipped, pct(skipped)),
    ]
    print_table(
        ("Status", "Count", "%"),
        table_rows,
        title=f"Total source rows: {total}\n",
    )
