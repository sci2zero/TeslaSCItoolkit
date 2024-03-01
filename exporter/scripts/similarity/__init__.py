import click
import exporter.similarity as similarity


@click.group(name="similarity")
def similarity_cli():
    pass


@similarity_cli.command()
def merge():
    """Merge datasets using similarity matching"""
    similarity.merge()


@similarity_cli.command()
def suggest():
    """Suggest transformations to apply to the dataset"""
    similarity.suggest()
