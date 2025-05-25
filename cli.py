import click

import app


@click.group()
def cli():
    pass


@cli.command()
@click.option("--dir", default="plugins")
def discover(dir: str):
    app.discover_plugins(dir)


if __name__ == "__main__":
    cli()
