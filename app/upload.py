import click
import structlog
from app import interface


def upload(
    plugin: interface.UploaderPlugin,
    table_name: str,
    table_description: str,
    bibcode: str,
    pub_name: str,
    pub_authors: list[str],
    pub_year: int,
    table_type: str,
) -> None:
    plugin.prepare()

    schema = plugin.get_schema()
    structlog.get_logger().info("Got schema", schema=schema)
    with click.progressbar(length=100, label="Upload") as bar:
        while (res := plugin.get_data()) is not None:
            data, progress = res

            percents = int(progress * 100)
            bar.update(percents)

    plugin.stop()
