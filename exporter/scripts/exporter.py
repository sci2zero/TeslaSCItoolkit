import click

from exporter.scripts.aggregate import aggregate
from exporter.scripts.apply import apply
from exporter.scripts.join import join
from exporter.scripts.include import include
from exporter.scripts.preview import preview
from exporter.scripts.release import release
from exporter.scripts.start import start
from exporter.scripts.suggest import suggest


@click.group()
def cli():
    pass


cli.add_command(aggregate)
cli.add_command(apply)
cli.add_command(join)
cli.add_command(include)
cli.add_command(preview)
cli.add_command(release)
cli.add_command(start)
cli.add_command(suggest)
