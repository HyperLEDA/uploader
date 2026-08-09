from collections.abc import Callable, Mapping, Sequence
from typing import Any

import astropy.units as u
import numpy as np
from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app.display import format_table
from uploader.app.lib.formula import (
    Expression,
    ExpressionEvaluationError,
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


def _format_unit(unit: u.UnitBase) -> str:
    text = f"{unit:s}".strip()
    return text if text else "dimensionless"


def _eval_context_suffix(expr: Expression, column_units: Mapping[str, str]) -> str:
    if not expr.referenced_columns:
        return ""
    parts = [f"{col}={column_units.get(col, '')!r}" for col in sorted(expr.referenced_columns)]
    return f"; columns: {', '.join(parts)}"


def _as_quantity(value: object) -> u.Quantity:
    if isinstance(value, u.Quantity):
        return value
    if isinstance(value, (int, float, np.number)) and not isinstance(value, bool):
        return float(value) * u.dimensionless_unscaled
    raise ValueError(f"numeric expression must evaluate to a quantity, got {type(value).__name__}")


def _scalar_numeric(quantity: u.Quantity) -> float:
    scalar = quantity.value
    if isinstance(scalar, np.ndarray):
        if scalar.shape != ():
            raise ValueError("expression must evaluate to a scalar value per row")
        return float(scalar)
    return float(scalar)


def _scalar_to_str(value: float | int | np.number) -> str:
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return str(numeric)


def _value_to_str(value: Value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, u.Quantity):
        scalar = value.value
        if isinstance(scalar, np.ndarray):
            if scalar.shape != ():
                raise ValueError("expression must evaluate to a scalar value per row")
            return _scalar_to_str(scalar.item())
        return _scalar_to_str(scalar)
    if value.shape != ():
        raise ValueError("expression must evaluate to a scalar value per row")
    item = value.item()
    if isinstance(item, str):
        return item
    if isinstance(item, (int, float, np.number)):
        return _scalar_to_str(item)
    raise ValueError(f"unsupported expression result type: {type(item).__name__}")


def _evaluate_numeric_field(
    expr: Expression,
    columns: Mapping[str, Value],
    column_units: Mapping[str, str],
    field: str,
    source: str,
    target_unit: str | None,
    data_type: DatatypeEnum,
) -> float | int:
    try:
        quantity = _as_quantity(evaluate(expr, columns))
    except (ExpressionEvaluationError, ValueError, u.UnitConversionError, u.UnitTypeError) as e:
        raise RuntimeError(
            f"failed to evaluate {field!r} ({source!r}){_eval_context_suffix(expr, column_units)}: {e}",
        ) from e
    try:
        if target_unit:
            converted = quantity.to(u.Unit(target_unit))
        else:
            converted = quantity.to(u.dimensionless_unscaled)
        value = _scalar_numeric(converted)
    except (ValueError, u.UnitConversionError, u.UnitTypeError) as e:
        raise RuntimeError(
            f"failed to convert {field!r} ({source!r}) "
            f"from {_format_unit(quantity.unit)} to {target_unit or 'dimensionless'}"
            f"{_eval_context_suffix(expr, column_units)}: {e}",
        ) from e
    if data_type in _INT_TYPES:
        return int(value)
    return value


def _evaluate_text_field(
    expr: Expression,
    columns: Mapping[str, Value],
    column_units: Mapping[str, str],
    field: str,
    source: str,
) -> str:
    try:
        return _value_to_str(evaluate(expr, columns))
    except (ExpressionEvaluationError, ValueError, TypeError) as e:
        raise RuntimeError(
            f"failed to evaluate {field!r} ({source!r}){_eval_context_suffix(expr, column_units)}: {e}",
        ) from e


def _build_column_values(
    row: Mapping[str, Any],
    referenced_columns: frozenset[str],
    column_units: Mapping[str, str],
) -> dict[str, Value]:
    return {col: column_quantity(row[col], column_units.get(col, "")) for col in referenced_columns}


def upload_catalog_columns(
    storage: PgStorage,
    table_name: str,
    catalog: str,
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
    all_needed_cols = set().union(*(expr.referenced_columns for expr in parsed.values())) if parsed else set()

    column_names, column_units = fetch_column_units(client, table_name)
    validate_columns(table_name, all_needed_cols, column_names)

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

            column_values = _build_column_values(row, frozenset(all_needed_cols), column_units)
            row_values: list[Any] = []
            try:
                for name in columns:
                    expr = parsed[name]
                    source = expressions[name]
                    if is_numeric_datatype(field_types[name]):
                        row_values.append(
                            _evaluate_numeric_field(
                                expr,
                                column_values,
                                column_units,
                                name,
                                source,
                                field_units.get(name),
                                field_types[name],
                            )
                        )
                    else:
                        row_values.append(
                            _evaluate_text_field(
                                expr,
                                column_values,
                                column_units,
                                name,
                                source,
                            )
                        )
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
