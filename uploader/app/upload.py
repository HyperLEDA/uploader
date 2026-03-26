import math
import traceback
from collections.abc import Callable
from typing import Any

import click

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
    emit_lines: Callable[[str], None] | None = None,
    on_progress_percent: Callable[[float], None] | None = None,
) -> tuple[int, dict[str, int]]:
    schema = plugin.get_schema()

    schema_rows = [(col.name, col.data_type.value) for col in schema]
    schema_text = format_table(
        ("Column", "Type"),
        schema_rows,
        title="\nSchema:",
        right_align_last_n=0,
        percent_last_column=False,
    )
    if emit_lines is not None:
        emit_lines(schema_text)
    else:
        click.echo(schema_text)

    if not dry_run:
        if bibcode == "":
            resp = handle_call(
                create_source.sync_detailed(
                    client=client,
                    body=models.CreateSourceRequest(
                        title=pub_name,
                        authors=pub_authors,
                        year=pub_year,
                    ),
                )
            )
            bibcode = resp.data.code
            log.logger.info("created internal source", id=bibcode)

        resp = handle_call(
            create_table.sync_detailed(
                client=client,
                body=models.CreateTableRequest(
                    table_name=table_name,
                    columns=schema,
                    bibcode=bibcode,
                    datatype=models.DataType[table_type],
                    description=table_description,
                ),
            )
        )

        log.logger.info("created table", table_id=resp.data.id)

    total_rows = 0
    non_null_by_column: dict[str, int] = {column.name: 0 for column in schema}
    prev_percent = 0

    def process_chunk(data: Any) -> None:
        nonlocal total_rows
        batch_rows = len(data)
        total_rows += batch_rows
        for column, non_null_count in data.count().to_dict().items():
            non_null_by_column[column] += int(non_null_count)

        if emit_lines is not None:
            emit_lines(f"batch: uploaded_rows={batch_rows}")

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
                    body=models.AddDataRequest(
                        table_name=table_name,
                        data=request_data,
                    ),
                )
            )

    data_iter = plugin.get_data()
    if on_progress_percent is not None:
        for data, progress in data_iter:
            process_chunk(data)
            percent = int(progress * 100)
            if percent != prev_percent:
                on_progress_percent(percent)
                prev_percent = percent
    else:
        with click.progressbar(length=100, label="Reading" if dry_run else "Upload") as bar:
            for data, progress in data_iter:
                process_chunk(data)
                percent = int(progress * 100)
                if percent != prev_percent:
                    bar.update(percent - prev_percent)
                    prev_percent = percent

    msg = f"\nTotal rows: {total_rows}"
    if emit_lines is not None:
        emit_lines(msg)
    else:
        click.echo(msg)
    return total_rows, non_null_by_column


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
    def emit(msg: str) -> None:
        report_func(report.LogEvent(message=msg))

    def on_progress(p: float) -> None:
        report_func(report.ProgressEvent(percent=p))

    plugin.prepare()
    try:
        total_rows, non_null_by_column = _upload(
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
            emit_lines=emit,
            on_progress_percent=on_progress,
        )
        non_null_rows = [(column, str(count)) for column, count in non_null_by_column.items()]
        non_null_table = format_table(
            ("Column", "Non-null values"),
            non_null_rows,
            title=f"\nNon-null values in {table_name}:",
            right_align_last_n=1,
            percent_last_column=False,
        )
        report_func(report.DoneEvent(message=f"Total rows: {total_rows}\n{non_null_table}"))
    finally:
        plugin.stop()
