import click
from pathlib import Path
import exporter.similarity as similarity


@click.group(name="similarity")
def similarity_cli():
    pass

@click.option(
    "-f",
    "--first-src",
    type=str,
    default=None,
    help="Path to the first data source",
)
@click.option(
    "-s",
    "--second-src",
    type=str,
    default=None,
    help="Path to the second data source",
)
@click.option(
    "-d",
    "--dest",
    type=str,
    default=None,
    help="Path to the destination of the combined data source",
)
@similarity_cli.command()
def merge(first_src, second_src, dest):
    """Merge datasets using similarity matching"""
    if first_src is not None and not Path(first_src).exists():
        raise click.UsageError(f"Path {first_src} not found. Does the path specified exist on disk?")

    if second_src is not None and not Path(second_src).exists():
        raise click.UsageError(f"Path {second_src} not found. Does the path specified exist on disk?")

    if dest is not None and not Path(dest).exists():
        raise click.UsageError(f"Path {dest} not found. Does the path specified exist on disk?")

    similarity.merge(first_src, second_src, dest)


@similarity_cli.command()
def suggest():
    """Suggest transformations to apply to the dataset"""
    similarity.suggest()
