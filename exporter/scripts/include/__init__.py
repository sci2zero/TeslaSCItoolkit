import click

import exporter.transformations as transformations
from exporter.scripts.context import DataSource


@click.command()
@click.option(
    "--cols",
    type=str,
    default=None,
    help="Comma separated columns to include in the export.",
)
def include(cols):
    """Columns to include in the final dataset"""
    data = DataSource.get()
    transformations.include(data, columns=cols.split(","))
    click.echo("Included World!")
