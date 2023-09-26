import click

import exporter.transformations as transformations

AGGREGATE_FUNCTIONS = ["count", "distinct", "max", "min", "sum", "avg"]


@click.command()
@click.argument("function", type=click.Choice(AGGREGATE_FUNCTIONS))
@click.option("-c", "--column", type=str, default=None, help="Column to group by.")
@click.option("-g", "--group", type=str, default=None, help="Column to group by.")
@click.option(
    "-a", "--alias", type=str, default=None, help="Column name for the result."
)
def aggregate(function, column, group, alias):
    """Run an aggregate function as it's typically understood by SQL semantics.
    Currently supported functions are: count, distinct, max, min, sum, avg
    """
    print(function, column, group, alias)
    transformations.aggregate(function, column, group, alias)
