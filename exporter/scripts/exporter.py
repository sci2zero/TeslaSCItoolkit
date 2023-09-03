import click

from exporter.scripts.aggregate import aggregate
from exporter.scripts.include import include
from exporter.scripts.start import start

@click.group()
def cli():
    pass

cli.add_command(start)
cli.add_command(aggregate)
cli.add_command(include)