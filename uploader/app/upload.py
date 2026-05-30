import math
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

import uploader.app.action_description as action_description
import uploader.app.report as report
from uploader.app import interface, log
from uploader.app.display import format_table
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi import models, types
from uploader.clients.gen.client.adminapi.api.default import (
    add_data,
    create_source,
    create_table,
)

MAX_UNIQUE_STATS_BYTES = 1024 * 1024 * 1024


@dataclass
class ColumnStats:
    non_null_count: int = 0
    unique_count: int | None = 0


def _estimate_row_size_bytes(schema: list[models.ColumnDescription]) -> int:
    bytes_per_type = {
        models.DatatypeEnum.INTEGER: 8,
        models.DatatypeEnum.LONG: 8,
        models.DatatypeEnum.DOUBLE: 8,
        models.DatatypeEnum.STRING: 64,
    }
    return sum(bytes_per_type.get(column.data_type, 64) for column in schema)


def upload(
    plugin: interface.UploaderSource,
    client: adminapi.AuthenticatedClient,
    *args,
    dry_run: bool = False,
    **kwargs,
) -> None:
    plugin.prepare()

    try:
        _upload(plugin, client, *args, dry_run=dry_run, **kwargs)
    except Exception as e:
        log.logger.error(
            "Encountered an unrecoverable error",
            error=e,
            traceback=traceback.format_exc(),
        )
    finally:
        plugin.stop()


def handle_call[T: Any](response: types.Response[T | models.HTTPValidationError]) -> T:
    if isinstance(response.parsed, models.HTTPValidationError):
        raise RuntimeError(response)
    if response.parsed is None:
        raise RuntimeError(f"Unable to get response: {response.content}")

    return response.parsed


def sanitize_value(val: Any) -> Any:
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def _upload(
    plugin: interface.UploaderSource,
    client: adminapi.AuthenticatedClient,
    table_name: str,
    table_description: str,
    bibcode: str,
    pub_name: str,
    pub_authors: list[str],
    pub_year: int,
    table_type: str,
    *,
    dry_run: bool = False,
    report_func: Callable[[report.Event], None],
) -> tuple[int, dict[str, ColumnStats]]:
    schema = plugin.get_schema()

    schema_rows = [(col.name, col.data_type.value) for col in schema]
    schema_text = format_table(
        ("Column", "Type"),
        schema_rows,
        title="\nSchema:",
        right_align_last_n=0,
        percent_last_column=False,
    )
    report_func(report.LogEvent(message=schema_text))

    if not dry_run:
        if bibcode == "":
            resp = handle_call(
                create_source.sync_detailed(
                    client=client,
                    body=action_description.apply(
                        models.CreateSourceRequest(
                            title=pub_name,
                            authors=pub_authors,
                            year=pub_year,
                        ),
                    ),
                )
            )
            bibcode = resp.data.code
            log.logger.info("created internal source", id=bibcode)

        resp = handle_call(
            create_table.sync_detailed(
                client=client,
                body=action_description.apply(
                    models.CreateTableRequest(
                        table_name=table_name,
                        columns=schema,
                        bibcode=bibcode,
                        datatype=models.DataType[table_type],
                        description=table_description,
                    ),
                ),
            )
        )

        log.logger.info("created table", table_id=resp.data.id)

    total_rows = 0
    estimated_total_rows = plugin.get_total_rows()
    column_stats: dict[str, ColumnStats] = {column.name: ColumnStats() for column in schema}
    estimated_row_size_bytes = _estimate_row_size_bytes(schema)
    estimated_total_size_bytes = estimated_total_rows * estimated_row_size_bytes
    skip_unique_stats = estimated_total_size_bytes >= MAX_UNIQUE_STATS_BYTES
    unique_values_by_column: dict[str, set[Any]] = {column.name: set() for column in schema}
    prev_percent = 0

    def process_chunk(data: pd.DataFrame) -> None:
        nonlocal total_rows
        batch_rows = len(data)
        total_rows += batch_rows
        for column, non_null_count in data.count().to_dict().items():
            column_stats[column].non_null_count += int(non_null_count)

        if not skip_unique_stats:
            for column in data.columns:
                unique_values_by_column[column].update(data[column].dropna().unique().tolist())

        if report_func is not None:
            report_func(report.LogEvent(message=f"batch: uploaded_rows={batch_rows}"))

        if not dry_run:
            request_data = []

            for _, row in data.iterrows():
                item = models.AddDataRequestDataItem()
                for col in data.columns:
                    item[col] = sanitize_value(row[col])
                request_data.append(item)

            _ = handle_call(
                add_data.sync_detailed(
                    client=client,
                    body=action_description.apply(
                        models.AddDataRequest(
                            table_name=table_name,
                            data=request_data,
                        ),
                    ),
                )
            )

    data_iter = plugin.get_data()
    size_estimate_msg = (
        "Estimated dataset size for unique values calculation: "
        f"{estimated_total_size_bytes // (1024 * 1024)} MB "
        f"(rows={estimated_total_rows}, est_row_bytes={estimated_row_size_bytes}, "
        f"threshold={MAX_UNIQUE_STATS_BYTES // (1024 * 1024)} MB)"
    )

    report_func(report.LogEvent(message=size_estimate_msg))

    for data, progress in data_iter:
        process_chunk(data)
        percent = int(progress * 100)
        if percent != prev_percent:
            report_func(report.ProgressEvent(percent=percent))
            prev_percent = percent

    report_func(report.LogEvent(message=f"\nTotal rows: {total_rows}"))

    for column_name in column_stats:
        if skip_unique_stats:
            column_stats[column_name].unique_count = None
        else:
            column_stats[column_name].unique_count = len(unique_values_by_column[column_name])

    return total_rows, column_stats


def upload_for_web(
    plugin: interface.UploaderSource,
    client: adminapi.AuthenticatedClient,
    table_name: str,
    table_description: str,
    bibcode: str,
    pub_name: str,
    pub_authors: list[str],
    pub_year: int,
    table_type: str,
    *,
    dry_run: bool = False,
    report_func: Callable[[report.Event], None],
) -> None:
    plugin.prepare()
    try:
        total_rows, column_stats = _upload(
            plugin,
            client,
            table_name,
            table_description,
            bibcode,
            pub_name,
            pub_authors,
            pub_year,
            table_type,
            dry_run=dry_run,
            report_func=report_func,
        )
        non_null_rows = []
        for column, stats in column_stats.items():
            unique_values = "-" if stats.unique_count is None else str(stats.unique_count)
            non_null_rows.append((column, str(stats.non_null_count), unique_values))
        non_null_table = format_table(
            ("Column", "Non-null values", "Unique values"),
            non_null_rows,
            title=f"\nStatistics for {table_name}:",
            right_align_last_n=2,
            percent_last_column=False,
        )
        report_func(report.DoneEvent(message=f"Total rows: {total_rows}\n{non_null_table}"))
    finally:
        plugin.stop()
