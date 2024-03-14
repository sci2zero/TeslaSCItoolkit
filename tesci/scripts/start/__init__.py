import click

from tesci.scripts.context import Config as config


@click.command()
@click.option("-d", "--dataset", type=click.STRING, help="Dataset to export")
@click.option("-o", "--output-dest", type=click.STRING, help="Output dataset name")
def start(dataset, output_dest):
    """Initializes the dataset."""
    config.init(dataset, output_dest)
    click.echo("Initialized the dataset")
