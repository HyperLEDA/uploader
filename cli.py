import inspect
import os
from collections.abc import Callable
from urllib.parse import quote_plus
from dataclasses import dataclass
from typing import Any

import click
import structlog

import app
from app.gen.client import adminapi

env_map = {
    "dev": "http://localhost:8080",
    "test": "https://leda.kraysent.dev",
    "prod": "https://leda.sao.ru",
}

db_dsn_map = {
    "dev": "postgresql://{user}:{password}@localhost:5432/hyperleda",
    "test": "postgresql://{user}:{password}@leda.kraysent.dev:5433/hyperleda",
    "prod": "postgresql://{user}:{password}@database.leda.sao.ru:5432/hyperleda",
}


@dataclass
class CommandContext:
    hyperleda_client: adminapi.AuthenticatedClient
    endpoint: str


@click.group(
    context_settings={
        "show_default": True,
    }
)
@click.option("--log-level", default="info", help="Log level")
@click.option(
    "--endpoint",
    help="HyperLeda API endpoint",
    type=click.Choice(env_map.keys()),
    default="prod",
)
@click.pass_context
def cli(ctx, log_level: str, endpoint: str) -> None:
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(log_level))

    ctx.obj = CommandContext(
        hyperleda_client=adminapi.AuthenticatedClient(
            base_url=env_map[endpoint],
            token="fake",  # TODO different for prod server
        ),
        endpoint=endpoint,
    )


@cli.command("name-checker")
@click.option("--user", required=True, help="Database user for the connection")
@click.option("--table-name", required=True, help="Rawdata table name")
@click.option("--column-name", required=True, help="Column containing the name")
@click.option("--batch-size", default=10000, type=int, help="Rows per batch")
@click.option("--print-unmatched", is_flag=True, help="Print each unmatched object name")
@click.pass_context
def name_checker(
    ctx: click.Context,
    user: str,
    table_name: str,
    column_name: str,
    batch_size: int,
    print_unmatched: bool,
) -> None:
    from app.name_checker import run_checker

    endpoint = ctx.obj.endpoint
    user_quoted = quote_plus(user)
    password = quote_plus(os.environ.get("DB_PASSWORD", ""))
    dsn = db_dsn_map[endpoint].format(user=user_quoted, password=password)
    run_checker(dsn, table_name, column_name, batch_size, print_unmatched=print_unmatched)


@cli.command()
@click.option("--plugin-dir", default="plugins", type=str)
def discover(plugin_dir: str) -> None:
    """
    Loads and checks all plugins for uploading. Plugins are .py files in the directory from `--plugin-dir` option.
    """
    app.discover_plugins(plugin_dir)


table_name_descr = "Table name is a primary identifier of the table in HyperLEDA. It usually is a machine-readable string that will later be used to do any alterations to the table. Example: sdss_dr12."
table_description_descr = (
    "Description of the table is a human-readable string that can later be used for searching of viewing the table."
)
bibcode_descr = "Bibcode is an identifier for the publication from the NASA ADS system https://ui.adsabs.harvard.edu/. It allows for easy search of the publication throughout a range of different sources."
pub_name_descr = "Name of the internal source. Can be a short description that represents where the data comes from."
pub_authors_descr = "Comma-separated list of authors of the internal source."
table_type_descr = "Type of the table to upload. Determines if the table is a compilation or the regular dataset. If unsure, leave blank."
auto_proceed_descr = "If set, will automatically accept all suggested defaults."


@cli.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
        "show_default": True,
    }
)
@click.option("--plugin-dir", default="plugins", type=str)
@click.option("--table-name", help=table_name_descr, default="")
@click.option("--table-description", help=table_description_descr, default="")
@click.option("--bibcode", help=bibcode_descr, default="")
@click.option("--pub-name", help=pub_name_descr, default="")
@click.option("--pub-authors", help=pub_authors_descr, default=[], multiple=True)
@click.option("--pub-year", type=int, default=0)
@click.option("--table-type", help=table_type_descr, default="")
@click.option("--auto-proceed", "-y", default=False, is_flag=True)
@click.argument("plugin-name", type=str)
@click.pass_context
def upload(
    ctx,
    plugin_dir: str,
    table_name: str,
    table_description: str,
    bibcode: str,
    pub_name: str,
    pub_authors: list[str],
    pub_year: int,
    table_type: str,
    auto_proceed: bool,
    plugin_name: str,
) -> None:
    plugins = app.discover_plugins(plugin_dir)
    plugin = get_plugin_instance(plugin_name, plugins, ctx.args)

    click.echo(
        "You will be prompted several questions about the table you want to upload. "
        "If there are square brackets before the colon ([example]), the question is not required and, if skipped, the default value from the brackets will be used as an answer.\n"
    )

    if table_name == "":
        default_table_name = None
        if isinstance(plugin, app.DefaultTableNamer):
            try:
                default_table_name = plugin.get_table_name()
            except Exception:
                app.logger.warning("failed to get default table name from plugin", plugin=plugin)

        table_name = question(
            "Enter table name",
            description=table_name_descr,
            default=default_table_name,
            skip_input=auto_proceed,
            transformer=str,
        )

    if table_description == "":
        default_description = None
        if isinstance(plugin, app.DescriptionProvider):
            try:
                default_description = plugin.get_description()
            except Exception as e:
                app.logger.warning(
                    "failed to get default table description from plugin",
                    plugin=plugin,
                    error=e,
                )

        table_description = question(
            "Enter description",
            description=table_description_descr,
            default=default_description,
            skip_input=auto_proceed,
            transformer=str,
        )

    if bibcode == "" and (pub_name == "" or len(pub_authors) == 0 or pub_year == 0):
        has_bibcode = question(
            "Do you have a bibcode for the publication? (y,n)",
            default="y",
            description=bibcode_descr,
            skip_input=auto_proceed,
            transformer=str,
        )

        if has_bibcode == "y":
            default_bibcode = None
            if isinstance(plugin, app.BibcodeProvider):
                try:
                    default_bibcode = plugin.get_bibcode()
                except Exception as e:
                    app.logger.warning(
                        "failed to get default bibcode from plugin",
                        plugin=plugin_name,
                        error=e,
                    )

            bibcode = question(
                "Enter bibcode",
                default=default_bibcode,
                skip_input=auto_proceed,
                transformer=str,
            )
        else:
            if pub_name == "":
                pub_name = question(
                    "Enter the name of the source",
                    description=pub_name_descr,
                    transformer=str,
                )

            if len(pub_authors) == 0:
                pub_authors = question(
                    "Enter list of authors of the source",
                    description=pub_authors_descr,
                    transformer=lambda s: [ss.strip() for ss in s.split(",")],
                )

            if pub_year == 0:
                pub_year = question("Enter year of the source", transformer=int)

    if table_type == "":
        table_type = question(
            "Enter the type of the table",
            description=table_type_descr,
            default="regular",
            skip_input=auto_proceed,
            transformer=str,
        )
        table_type = table_type.upper()

    click.echo("\nThanks! The table will be uploaded with the following parameters: ")
    click.echo(parameter("Table name", table_name))
    click.echo(parameter("Table description", table_description))
    if bibcode != "":
        click.echo(parameter("Bibcode", bibcode))
    else:
        click.echo(parameter("Source name", pub_name))
        click.echo(parameter("Source authors", ", ".join(pub_authors)))
        click.echo(parameter("Source year", str(pub_year)))
    click.echo(parameter("Table type", table_type))

    if not auto_proceed:
        auto_proceed = question("Proceed? (y,n)", default="y", transformer=lambda s: s == "y")

    if auto_proceed:
        app.upload(
            plugin,
            ctx.obj.hyperleda_client,
            table_name,
            table_description,
            bibcode,
            pub_name,
            pub_authors,
            pub_year,
            table_type,
        )


def get_plugin_instance(
    plugin_name: str,
    plugins: dict[str, type[app.UploaderPlugin]],
    args: list[Any],
) -> app.UploaderPlugin:
    plugin_class = plugins[plugin_name]

    try:
        return plugin_class(*args)
    except TypeError:
        pass

    s = inspect.signature(plugin_class)
    required_args = []

    for arg_name, arg in s.parameters.items():
        if arg.default is inspect.Parameter.empty:
            required_args.append(arg_name)

    raise RuntimeError(
        f"Plugin {plugin_name} has {len(required_args)} required arguments ({required_args}). {len(args)} were given."
    )


def parameter(name: str, value: str) -> str:
    return f"{click.style(f'{name}', bold=True)}: {value}"


def question[T: Any](
    question: str,
    transformer: Callable[[str], T],
    *,
    description: str = "",
    default: str | None = None,
    skip_input: bool = False,
) -> T:
    if default is not None:
        question += f" [{default}]"

    if skip_input and default is not None:
        result = ""
    else:
        if description != "":
            click.echo(f"\n{description}")

        result = input(click.style(f"{question}: ", bold=True))

    if result == "":
        if default is not None:
            return transformer(default)
    else:
        return transformer(result)

    while result == "" and not skip_input:
        click.echo("This is a required parameter. Please, try again.")

        result = input(click.style(f"{question}: ", bold=True))

    return transformer(result)


if __name__ == "__main__":
    cli()
