import click

import exporter.transformations as transformations

AGGREGATE_FUNCTIONS = ["count", "distinct", "max", "min", "sum", "avg"]


@click.command()
@click.argument("function", type=click.Choice(AGGREGATE_FUNCTIONS))
@click.option("-c", "--column", type=str, default=None, help="Column to group by.")
@click.option(
    "-g",
    "--group",
    type=str,
    default=None,
    help="Comma separated list of columns to group by.",
)
@click.option(
    "-a", "--alias", type=str, default=None, help="Column name for the result."
)
def aggregate(function, column, group, alias):
    """Run an aggregate function as it's typically understood by SQL semantics.
    Currently supported functions are: count, distinct, max, min, sum, avg
    """
    message = f"Successfully ran function {function.upper()}"
    if not group and not column:
        raise click.UsageError("You must specify either a column or a group.")
    if group is not None:
        message += f" grouped by {group}"
        group = group.split(",")
    if not column and group is not None:
        column = group[0] if isinstance(group, list) else group

    message += f" on column {column}"

    transformations.aggregate(function, column, group, alias)
    click.echo(message)
