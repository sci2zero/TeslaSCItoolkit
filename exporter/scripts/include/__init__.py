import click

from exporter.scripts.context import DataSource


@click.command()
@click.option("--cols", default=None, help="Columns to include in the export.")
def include(cols):
    """Example include."""
    breakpoint()
    data = DataSource.get()
    print(data.source)
    click.echo('Included World!')