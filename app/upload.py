import math
import traceback
from collections.abc import Callable
from typing import Any

import click

import app.report_events as report_events
from app import interface, log
from app.display import format_table
from app.gen.client import adminapi
from app.gen.client.adminapi import models, types
from app.gen.client.adminapi.api.default import (
    add_data,
    create_source,
    create_table,
)


def upload(
    plugin: interface.UploaderPlugin,
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
    plugin: interface.UploaderPlugin,
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
) -> int:
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
    prev_percent = 0

    def process_chunk(data: Any) -> None:
        nonlocal total_rows
        total_rows += len(data)

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
    return total_rows


def upload_for_web(
    plugin: interface.UploaderPlugin,
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
    report: Callable[[report_events.ReportEvent], None],
) -> None:
    def emit(msg: str) -> None:
        report(report_events.ReportLog(message=msg))

    def on_progress(p: float) -> None:
        report(report_events.ReportProgress(percent=p))

    plugin.prepare()
    try:
        total_rows = _upload(
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
        report(report_events.ReportDone(message=f"Total rows: {total_rows}"))
    finally:
        plugin.stop()
