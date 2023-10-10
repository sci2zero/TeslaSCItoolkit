import click

import exporter.mirrors as mirrors

@click.command()
def publish():
    mirrors.publish()