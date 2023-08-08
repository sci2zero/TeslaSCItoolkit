import click

from exporter.scripts.aggregate import aggregate

@click.group()
def cli():
    pass

cli.add_command(aggregate)