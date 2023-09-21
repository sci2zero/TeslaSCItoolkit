import click

import exporter.transformations as transformations


@click.command()
def preview():
    """Preview the changes that will be applied to the dataset"""
    transformations.preview()
