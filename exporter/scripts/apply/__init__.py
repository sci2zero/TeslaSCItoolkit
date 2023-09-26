import click

import exporter.transformations as transformations


@click.command()
@click.option("--ci", default=False, is_flag=True, help="Run in CI mode")
def apply(ci):
    """Apply the changes to the destination dataset"""
    transformations.preview()

    if not ci:
        if click.confirm("Are you sure you want to apply these changes?"):
            transformations.apply()
        else:
            click.echo("Aborted")
            return
    else:
        transformations.apply()

    click.echo("Applied changes")
