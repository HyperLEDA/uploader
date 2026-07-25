import csv
import pathlib
from collections.abc import Callable
from typing import Any

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
from psycopg import sql

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app import log
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
from uploader.app.lib.table import fetch_column_units
from uploader.app.storage import PgStorage
from uploader.app.structured.designations.rules import RULES, match
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import save_structured_data
from uploader.clients.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)

CHART_FIGSIZE = (8, 6)


def _rule_distribution_bars(
    rule_counts: dict[str, int],
    unmatched: int,
) -> list[tuple[str, int]]:
    sorted_rules = sorted(rule_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    top = [(name, count) for name, count in sorted_rules[:10] if count > 0]
    other_total = sum(count for _, count in sorted_rules[10:])
    bars: list[tuple[str, int]] = list(top)
    if other_total > 0:
        bars.append(("(other rules)", other_total))
    if unmatched > 0:
        bars.append(("(unparsed)", unmatched))
    return bars


def _emit_rule_distribution_image(
    report_func: Callable[[report.Event], None],
    rule_counts: dict[str, int],
    unmatched: int,
    *,
    caption: str,
) -> None:
    bars = _rule_distribution_bars(rule_counts, unmatched)
    if not bars:
        return
    labels = [name for name, _ in bars]
    counts = [count for _, count in bars]
    fig, ax = plt.subplots(figsize=CHART_FIGSIZE)
    ax.barh(labels, counts)
    ax.invert_yaxis()
    ax.set_xlabel("Count")
    ax.set_title("Designation rule distribution")
    report_func(report.image_event_from_figure(fig, caption=caption))


def _report_batch_progress(
    report_func: Callable[[report.Event], None],
    *,
    rows_read: int,
    total_so_far: int,
    matched: int,
    unmatched: int,
    progress_pct: int,
    rule_counts: dict[str, int],
) -> None:
    report_func(report.ProgressEvent(percent=min(99, progress_pct)))
    report_func(
        report.LogEvent(
            message=(
                f"batch: rows_read={rows_read} cumulative_names={total_so_far} matched={matched} unmatched={unmatched}"
            ),
        ),
    )
    _emit_rule_distribution_image(
        report_func,
        rule_counts,
        unmatched,
        caption=f"{total_so_far} names processed",
    )


def _report_rule_distribution(
    report_func: Callable[[report.Event], None],
    rule_counts: dict[str, int],
    unmatched: int,
    total: int,
) -> None:
    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows = [
        (name, rule_counts[name], pct(rule_counts[name]))
        for name in sorted(rule_counts.keys(), key=lambda n: (-rule_counts[n], n))
        if rule_counts[name] > 0
    ]
    table_rows.append(("(no rule matched)", unmatched, pct(unmatched)))

    report_func(report.ProgressEvent(percent=100))

    _emit_rule_distribution_image(
        report_func,
        rule_counts,
        unmatched,
        caption=f"Final: {total} names",
    )

    summary = format_table(
        ("Rule", "Count", "%"),
        table_rows,
        title=f"Total names: {total}\n",
    )
    report_func(report.DoneEvent(message=summary))


def _validate_columns(
    table_name: str,
    needed_cols: set[str],
    column_names: set[str],
) -> None:
    missing = sorted(col for col in needed_cols if col not in column_names)
    if missing:
        raise RuntimeError(f"Table {table_name} has no column(s): {missing}")


def _build_column_values(
    row: dict[str, Any],
    referenced_columns: frozenset[str],
    column_units: dict[str, str],
) -> dict[str, Value]:
    return {col: column_quantity(row[col], column_units.get(col, "")) for col in referenced_columns}


def _designation_string(value: Value) -> str:
    if isinstance(value, str):
        return value.strip()
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
    print_unmatched: bool = False,
    output_file: str = "",
    report_func: Callable[[report.Event], None],
) -> int:
    parsed = parse(expression)
    needed_cols = set(parsed.referenced_columns)
    column_names, column_units = fetch_column_units(client, table_name)
    _validate_columns(table_name, needed_cols, column_names)

    rule_counts: dict[str, int] = {r.name: 0 for r in RULES}
    unmatched = 0
    total_count = 0
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
                    unmatched += 1
                    continue
                name_str = _evaluate_designation(parsed, row, column_units)
                if not name_str:
                    unmatched += 1
                    continue
                match_result = match(name_str)
                if match_result is not None:
                    transformed, rule_name = match_result
                    rule_counts[rule_name] += 1
                else:
                    unmatched += 1
                    transformed = name_str
                    if print_unmatched:
                        report_func(report.LogEvent(message=name_str))
                batch_ids.append(internal_id)
                batch_names.append([transformed])

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

            processed_rows += len(rows)
            total_so_far = sum(rule_counts.values()) + unmatched

            def total_pct(n: int, t: int = total_so_far) -> float:
                return (100.0 * n / t) if t else 0.0

            log.logger.info(
                "processed batch",
                total=total_so_far,
                matched=sum(rule_counts.values()),
                matched_pct=round(total_pct(sum(rule_counts.values())), 1),
                unmatched=unmatched,
                unmatched_pct=round(total_pct(unmatched), 1),
            )
            progress_pct = int(100 * processed_rows / total_count) if total_count else 0
            _report_batch_progress(
                report_func,
                rows_read=len(rows),
                total_so_far=total_so_far,
                matched=sum(rule_counts.values()),
                unmatched=unmatched,
                progress_pct=progress_pct,
                rule_counts=rule_counts,
            )
    finally:
        if output_writer is not None:
            output_writer.close()

    total = sum(rule_counts.values()) + unmatched
    _report_rule_distribution(report_func, rule_counts, unmatched, total)

    return total
