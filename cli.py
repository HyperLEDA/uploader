import click
import structlog

import app


@click.group()
def cli():
    pass


@cli.command()
@click.option("--plugin-dir", default="plugins", type=str)
def discover(plugin_dir: str) -> None:
    app.discover_plugins(plugin_dir)


@cli.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    }
)
@click.option("--plugin-dir", default="plugins", type=str)
@click.argument("plugin-name", type=str)
@click.pass_context
def upload(ctx: click.Context, plugin_dir: str, plugin_name: str) -> None:
    plugins = app.discover_plugins(plugin_dir)

    plugin = plugins[plugin_name](*ctx.args)

    plugin.prepare()

    schema = plugin.get_schema()
    structlog.get_logger().info("Got schema", schema=schema)

    while (data := plugin.get_data()) is not None:
        structlog.get_logger().info("Got data", data=data)

    plugin.stop()


if __name__ == "__main__":
    cli()
