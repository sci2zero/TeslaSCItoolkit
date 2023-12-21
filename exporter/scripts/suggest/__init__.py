import click

import exporter.suggestions as suggestions


@click.command()
def suggest():
    """Suggest transformations to apply to the dataset"""
    suggestions.preview()
