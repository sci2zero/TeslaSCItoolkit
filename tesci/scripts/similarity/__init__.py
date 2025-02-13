import click
from pathlib import Path
import tesci.similarity as similarity


@click.group(name="similarity")
def similarity_cli():
    pass

@click.option(
    "-s",
    "--src",
    default=None,
    multiple=True,
    help="Path to the the data sources",
)
@click.option(
    "-d",
    "--dest",
    type=str,
    default=None,
    help="Path to the destination of the combined data source",
)
@similarity_cli.command()
def merge(src, dest):
    """Merge datasets using similarity matching"""
    sources = src
    if sources is None or len(sources) == 0:
        raise click.UsageError("At least one source path must be specified")
    for src in sources:
        if not Path(src).exists():
            raise click.UsageError(f"Path {src} not found. Does the path specified exist on disk?")

    if dest is not None and not Path(dest).exists():
        raise click.UsageError(f"Path {dest} not found. Does the path specified exist on disk?")
    similarity.merge(sources, dest)


@similarity_cli.command()
def suggest():
    """Suggest transformations to apply to the dataset"""
    similarity.suggest()
