import click

from exporter.scripts.context import CSV

@click.command()
@click.option("-d", "--dataset", type=click.Path(exists=True))
def start(dataset):
    """Example start."""
    CSV(dataset)
    
    click.echo('Initialized the dataset')