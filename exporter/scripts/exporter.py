import click

from exporter.scripts.aggregate import aggregate
from exporter.scripts.apply import apply
from exporter.scripts.include import include
from exporter.scripts.start import start
from exporter.scripts.preview import preview


@click.group()
def cli():
    pass


cli.add_command(aggregate)
cli.add_command(apply)
cli.add_command(start)
cli.add_command(include)
cli.add_command(preview)
