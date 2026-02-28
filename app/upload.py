import math
from typing import Any

import click

from app import interface, log
from app.display import print_table
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
        log.logger.error("Encountered an unrecoverable error", error=e)
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
) -> None:
    schema = plugin.get_schema()

    schema_rows = [(col.name, col.data_type.value) for col in schema]
    print_table(
        ("Column", "Type"),
        schema_rows,
        title="\nSchema:",
        right_align_last_n=0,
        percent_last_column=False,
    )

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

    with click.progressbar(length=100, label="Reading" if dry_run else "Upload") as bar:
        prev_percent = 0
        for data, progress in plugin.get_data():
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

            percent = int(progress * 100)
            if percent != prev_percent:
                bar.update(percent - prev_percent)
                prev_percent = percent

    click.echo(f"\nTotal rows: {total_rows}")
