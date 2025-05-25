import click
import hyperleda
import structlog
from app import interface

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def upload(
    plugin: interface.UploaderPlugin,
    client: hyperleda.HyperLedaClient,
    *args,
    **kwargs,
) -> None:
    plugin.prepare()

    try:
        _upload(plugin, client, *args, **kwargs)
    except Exception as e:
        logger.error("Encountered an unrecoverable error", error=e)
    finally:
        plugin.stop()


def _upload(
    plugin: interface.UploaderPlugin,
    client: hyperleda.HyperLedaClient,
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
        bibcode = client.create_internal_source(pub_name, pub_authors, pub_year)
        logger.info("created internal source", id=bibcode)

    table_id = client.create_table(
        hyperleda.CreateTableRequestSchema(
            table_name,
            schema,
            bibcode,
            hyperleda.Datatype[table_type],
            table_description,
        )
    )
    logger.info("created table", table_id=table_id)
    logger.info("starting upload")

    with click.progressbar(length=100, label="Upload") as bar:
        while (res := plugin.get_data()) is not None:
            data, progress = res

            client.add_data(table_id, data)

            percents = int(progress * 100)
            bar.update(percents)
