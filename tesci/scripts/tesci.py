import click

from tesci.scripts.aggregate import aggregate
from tesci.scripts.apply import apply
from tesci.scripts.download import download
from tesci.scripts.similarity import similarity_cli, merge, suggest
from tesci.scripts.join import join
from tesci.scripts.include import include
from tesci.scripts.preview import preview
from tesci.scripts.release import release
from tesci.scripts.start import start


@click.group()
def cli():
    pass


cli.add_command(aggregate)
cli.add_command(apply)
cli.add_command(download)
cli.add_command(similarity_cli)
similarity_cli.add_command(merge)
similarity_cli.add_command(suggest)
cli.add_command(join)
cli.add_command(include)
cli.add_command(preview)
cli.add_command(release)
cli.add_command(start)
