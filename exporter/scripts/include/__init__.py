import click

import exporter.transformations as transformations


@click.command()
@click.option(
    "--columns",
    type=str,
    default=None,
    help="Comma separated columns to include in the export.",
)
def include(columns):
    """Columns to include in the final dataset"""
    transformations.include(columns=columns.split(","))
