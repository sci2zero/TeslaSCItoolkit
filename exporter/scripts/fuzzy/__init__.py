import click
import exporter.fuzzy as fuzzy


@click.group(name="fuzzy")
def fuzzy_cli():
    pass


@fuzzy_cli.command()
def merge():
    """Merge datasets using fuzzy matching"""
    fuzzy.merge()


@fuzzy_cli.command()
def suggest():
    """Suggest transformations to apply to the dataset"""
    fuzzy.suggest()
