import click

import tesci.api.api as api


@click.command()
@click.option(
    "-d",
    "--dest",
    type=str,
    default=None,
    help="Path to the destination to downloaded source",
)
def download(dest):
    """Download the data from the provider and save it to the destination"""
    api.download(dest)
