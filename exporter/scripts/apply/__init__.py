import click

import exporter.transformations as transformations


@click.command()
def apply():
    """Apply the changes to the destination dataset"""
    transformations.preview()
    
    if click.confirm("Are you sure you want to apply these changes?"):
        transformations.apply()
        click.echo("Applied changes")
    else:
        click.echo("Aborted")
