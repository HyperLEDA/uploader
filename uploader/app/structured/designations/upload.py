import csv
import pathlib
from collections.abc import Callable
from typing import Any

import astropy.units as u
import numpy as np
from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app import log
from uploader.app.lib.formula import (
    Expression,
    ExpressionEvaluationError,
    TextValue,
    Value,
    column_quantity,
    evaluate,
    parse,
)
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.lib.table import fetch_column_units, validate_columns
from uploader.app.storage import PgStorage
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)


def _build_column_values(
    row: dict[str, Any],
    referenced_columns: frozenset[str],
    column_units: dict[str, str],
) -> dict[str, Value]:
    return {col: column_quantity(row[col], column_units.get(col, "")) for col in referenced_columns}


def _designation_string(value: Value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, TextValue):
        return value.data.strip()
    if isinstance(value, u.Quantity):
        scalar = value.value
        if isinstance(scalar, np.ndarray) and scalar.shape != ():
            raise RuntimeError("designation expression must evaluate to a scalar value per row")
        return str(scalar).strip()
    raise RuntimeError("designation expression must evaluate to a scalar value per row")


def _evaluate_designation(
    parsed: Expression,
    row: dict[str, Any],
    column_units: dict[str, str],
) -> str:
    columns = _build_column_values(row, parsed.referenced_columns, column_units)
    try:
        return _designation_string(evaluate(parsed, columns))
    except ExpressionEvaluationError as e:
        raise RuntimeError(
            f"failed to evaluate expression for row {row['hyperleda_internal_id']}: {e}",
        ) from e


class _DesignationOutputWriter:
    def __init__(self, path: str) -> None:
        output_path = pathlib.Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = output_path.open("w", newline="")
        self._writer = csv.writer(self._file)
        self._writer.writerow(["id", "designation"])

    def write_batch(self, ids: list[str], designations: list[list[str]]) -> None:
        for record_id, designation in zip(ids, designations, strict=True):
            self._writer.writerow([record_id, designation[0]])

    def close(self) -> None:
        self._file.close()


def upload_designations(
    storage: PgStorage,
    table_name: str,
    expression: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    output_file: str = "",
    report_func: Callable[[report.Event], None],
) -> int:
    parsed = parse(expression)
    needed_cols = set(parsed.referenced_columns)
    column_names, column_units = fetch_column_units(client, table_name)
    validate_columns(table_name, needed_cols, column_names)

    skipped = 0
    uploaded = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0

    processed_rows = 0
    output_writer = _DesignationOutputWriter(output_file) if output_file else None

    try:
        for rows in rawdata_batches(storage, table_name, sorted(needed_cols), batch_size):
            batch_ids: list[str] = []
            batch_names: list[list[str]] = []

            for row in rows:
                internal_id = row["hyperleda_internal_id"]
                if any(row[col] is None for col in needed_cols):
                    skipped += 1
                    continue
                name_str = _evaluate_designation(parsed, row, column_units)
                if not name_str:
                    skipped += 1
                    continue
                batch_ids.append(internal_id)
                batch_names.append([name_str])

            if write and batch_ids:
                handle_call(
                    save_structured_data.sync_detailed(
                        client=client,
                        body=action_description.apply(
                            SaveStructuredDataRequest(
                                catalog="designation",
                                columns=["design"],
                                ids=batch_ids,
                                data=batch_names,
                            ),
                        ),
                    )
                )

            if output_writer is not None and batch_ids:
                output_writer.write_batch(batch_ids, batch_names)

            uploaded += len(batch_ids)
            processed_rows += len(rows)
            log.logger.info(
                "processed batch",
                uploaded=uploaded,
                skipped=skipped,
            )
            progress_pct = int(100 * processed_rows / total_count) if total_count else 0
            report_func(report.ProgressEvent(percent=min(99, progress_pct)))
            report_func(
                report.LogEvent(
                    message=(f"batch: rows_read={len(rows)} uploaded={uploaded} skipped={skipped}"),
                ),
            )
    finally:
        if output_writer is not None:
            output_writer.close()

    report_func(report.ProgressEvent(percent=100))
    report_func(
        report.DoneEvent(
            message=f"Total names: {uploaded + skipped}\nUploaded: {uploaded}\nSkipped: {skipped}",
        ),
    )

    return uploaded + skipped
