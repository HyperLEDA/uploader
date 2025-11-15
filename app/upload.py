import math
from typing import Any

import click

from app import interface, log
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
    **kwargs,
) -> None:
    plugin.prepare()

    try:
        _upload(plugin, client, *args, **kwargs)
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
) -> None:
    schema = plugin.get_schema()

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
    log.logger.info("starting upload")

    with click.progressbar(length=100, label="Upload") as bar:
        prev_percent = 0
        for data, progress in plugin.get_data():
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
