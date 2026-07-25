from collections.abc import Callable, Mapping, Sequence
from typing import Any

import astropy.units as u
from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app.display import format_table
from uploader.app.lib.expression import Expression, eval_context_suffix, format_unit, parse
from uploader.app.lib.rawdata import rawdata_batches
from uploader.app.lib.table import fetch_column_units
from uploader.app.storage import PgStorage
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.datatype_enum import DatatypeEnum
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from uploader.clients.gen.client.adminapi.models.save_structured_data_request_units import (
    SaveStructuredDataRequestUnits,
)
from uploader.clients.gen.client.adminapi.types import UNSET

_FLOAT_TYPES = frozenset(
    {
        DatatypeEnum.DOUBLE,
        DatatypeEnum.DOUBLE_PRECISION,
        DatatypeEnum.FLOAT,
        DatatypeEnum.NUMBER,
        DatatypeEnum.REAL,
    }
)
_INT_TYPES = frozenset(
    {
        DatatypeEnum.INT,
        DatatypeEnum.INTEGER,
        DatatypeEnum.LONG,
        DatatypeEnum.POSITIVEINTEGER,
        DatatypeEnum.SHORT,
        DatatypeEnum.SMALLINT,
        DatatypeEnum.UNSIGNEDBYTE,
        DatatypeEnum.UNSIGNEDINT,
        DatatypeEnum.UNSIGNEDLONG,
        DatatypeEnum.UNSIGNEDSHORT,
    }
)
_NUMERIC_TYPES = _FLOAT_TYPES | _INT_TYPES


def is_numeric_datatype(data_type: DatatypeEnum) -> bool:
    return data_type in _NUMERIC_TYPES


def _evaluate_numeric_field(
    expr: Expression,
    values: dict[str, float],
    column_units: dict[str, str],
    field: str,
    source: str,
    target_unit: str | None,
    data_type: DatatypeEnum,
) -> float | int:
    target = target_unit if target_unit is not None else ""
    try:
        quantity = expr.evaluate(values, column_units)
    except (ValueError, u.UnitConversionError, u.UnitTypeError) as e:
        raise RuntimeError(
            f"failed to evaluate {field!r} ({source!r}){eval_context_suffix(expr, column_units)}: {e}",
        ) from e
    try:
        if target:
            value = float(quantity.to(u.Unit(target)).value)
        else:
            value = float(quantity.to(u.dimensionless_unscaled).value)
    except (u.UnitConversionError, u.UnitTypeError) as e:
        raise RuntimeError(
            f"failed to convert {field!r} ({source!r}) "
            f"from {format_unit(quantity.unit)} to {target or 'dimensionless'}"
            f"{eval_context_suffix(expr, column_units)}: {e}",
        ) from e
    if data_type in _INT_TYPES:
        return int(value)
    return value


def upload_catalog_columns(
    storage: PgStorage,
    table_name: str,
    catalog: str,
    column_map: Mapping[str, str],
    expressions: Mapping[str, str],
    field_types: Mapping[str, DatatypeEnum],
    field_units: Mapping[str, str],
    columns: Sequence[str],
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    report_func: Callable[[report.Event], None],
) -> int:
    if not columns:
        raise RuntimeError("No catalog columns selected for upload")

    parsed = {field: parse(source) for field, source in expressions.items()}
    expr_cols = set().union(*(expr.referenced_columns for expr in parsed.values())) if parsed else set()
    mapped_cols = set(column_map.values())
    all_needed_cols = expr_cols | mapped_cols

    column_names, column_units = fetch_column_units(client, table_name)
    missing = sorted(col for col in all_needed_cols if col not in column_names)
    if missing:
        raise RuntimeError(f"Table {table_name} has no column(s): {missing}")

    units_payload: dict[str, str] = {name: field_units[name] for name in columns if name in field_units}
    units = SaveStructuredDataRequestUnits.from_dict(units_payload) if units_payload else UNSET

    uploaded = 0
    skipped = 0
    cnt = storage.query(
        sql.SQL("SELECT COUNT(*) AS cnt FROM rawdata.{}").format(sql.Identifier(table_name)),
        (),
    )
    total_count = int(cnt[0]["cnt"]) if cnt else 0
    processed_rows = 0

    fetch_columns = sorted(all_needed_cols)
    for rows in rawdata_batches(storage, table_name, fetch_columns, batch_size):
        batch_ids: list[str] = []
        batch_data: list[list[Any]] = []

        for row in rows:
            if any(row[col] is None for col in all_needed_cols):
                skipped += 1
                continue

            values = {col: float(row[col]) for col in expr_cols}
            row_values: list[Any] = []
            try:
                for name in columns:
                    if name in parsed:
                        row_values.append(
                            _evaluate_numeric_field(
                                parsed[name],
                                values,
                                column_units,
                                name,
                                expressions[name],
                                field_units.get(name),
                                field_types[name],
                            )
                        )
                    else:
                        row_values.append(str(row[column_map[name]]))
            except RuntimeError as e:
                raise RuntimeError(
                    f"failed to evaluate expressions for row {row['hyperleda_internal_id']}: {e}",
                ) from e

            batch_ids.append(row["hyperleda_internal_id"])
            batch_data.append(row_values)
            uploaded += 1

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=action_description.apply(
                        SaveStructuredDataRequest(
                            catalog=catalog,
                            columns=list(columns),
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

    total = uploaded + skipped

    def row_pct_label(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    report_func(report.ProgressEvent(percent=100))
    summary = format_table(
        ("Status", "Count", "%"),
        [
            ("Uploaded", uploaded, row_pct_label(uploaded)),
            ("Skipped (null)", skipped, row_pct_label(skipped)),
        ],
        title=f"Total rows: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))
    return total
