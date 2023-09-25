import click

import exporter.transformations as transformations

AGGREGATE_FUNCTIONS = ["count", "distinct", "max", "min", "sum", "avg"]


@click.command()
@click.argument("type", type=click.Choice(AGGREGATE_FUNCTIONS))
@click.option("--group", type=str, default=None, help="Column to group by.")
@click.option("--as", type=str, default=None, help="Column name for the result.")
def aggregate(type, group, as_name):
    """Example aggregate."""
    print(type, group, as_name)
    transformations.aggregate(type, group, as_name)
    click.echo("Hello World!")
