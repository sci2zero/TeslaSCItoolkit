import click

from exporter.scripts.context import Config

@click.command()
@click.option("-d", "--dataset", type=click.STRING, help="Dataset to export")
def start(dataset):
    """Initializes the dataset."""
    Config.init(dataset)
    click.echo('Initialized the dataset')