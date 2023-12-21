import os

import click

import exporter.transformations as transformations


@click.command()
def apply():
    """Apply the changes to the destination dataset"""
    # transformations.preview()

    is_ci = os.environ.get("EXPORTER_RUN_FROM_CI", None)
    if not is_ci:
        if click.confirm("Are you sure you want to apply these changes?"):
            transformations.apply()
        else:
            click.echo("Aborted")
            return
    else:
        transformations.apply()

    click.echo("Applied changes")
