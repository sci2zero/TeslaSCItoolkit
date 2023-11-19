import click
import os

import exporter.release as publish


@click.command()
def release():
    """Publish the dataset to the repositories specified in the configuration file"""
    is_ci = os.environ.get("EXPORTER_RUN_FROM_CI", None)
    if not is_ci:
        if click.confirm(
            "Are you sure you want to release exported data to remote repositories?"
        ):
            publish.release()
        else:
            click.echo("Aborted")
            return
    else:
        publish.release()

    click.echo("Release complete")
