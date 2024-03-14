import click
import os

import tesci.release as publish


@click.command()
def release():
    """Publish the dataset to the repositories specified in the configuration file"""
    is_ci = os.environ.get("EXPORTER_RUN_FROM_CI", None)
    strategy = os.environ.get("EXPORTER_RELEASE_STRATEGY", None)
    if not is_ci:
        if click.confirm(
            "Are you sure you want to release exported data to remote repositories?"
        ):
            publish.release(strategy=None)
        else:
            click.echo("Aborted")
            return
    else:
        publish.release(strategy=strategy)

    click.echo("Release complete")
