import click
import hyperleda
from app import interface, log


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
        log.logger.error("Encountered an unrecoverable error", error=e)
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
        log.logger.info("created internal source", id=bibcode)

    table_id = client.create_table(
        hyperleda.CreateTableRequestSchema(
            table_name,
            schema,
            bibcode,
            hyperleda.Datatype[table_type],
            table_description,
        )
    )
    log.logger.info("created table", table_id=table_id)
    log.logger.info("starting upload")

    with click.progressbar(length=100, label="Upload") as bar:
        prev_percent = 0
        for data, progress in plugin.get_data():
            client.add_data(table_id, data)

            percent = int(progress * 100)
            if percent != prev_percent:
                bar.update(percent - prev_percent)
                prev_percent = percent
