import logging
import pandas as pd
import attrs

from typing import Any
from exporter.scripts.context import DataSource, Config
from exporter.types import Aggregate

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def aggregate(
    function: str, column: str | None, group: list[str] | None, alias: str | None
) -> None:
    data = DataSource.get()
    config = Config()

    if group is not None:
        validate_columns(data, group)

    if "aggregate" not in config.content:
        config.content["aggregate"] = []

    aggregate_dict = attrs.asdict(Aggregate(function, column, alias))

    if group is not None:
        aggregate_dict["grouped"] = group

    config.content["aggregate"].append(aggregate_dict)
    config.write()


def preview() -> Any:
    """Preview the changes that will be applied to the dataset"""
    data = DataSource.get()
    config = Config()

    if "include" not in config.content:
        logging.info("No columns to include in the dataset.")
        # TODO: what about aggregate columns existing?
        return

    columns = config.content["include"]
    validate_columns(data, columns)
    logging.info("Included columns: %s", columns)

    aggregations = config.content["aggregate"]
    logging.info("Aggregations to apply: %s", aggregations)

    print("Preview of the dataset:")
    df = preview_dataset(data, config)
    print(df.head(10))


def include(columns: list[str]) -> None:
    """Columns to include in the final dataset"""
    data = DataSource.get()
    validate_columns(data, columns)
    add_columns(columns)

    logging.info("Included columns: %s", columns)


def validate_columns(data: DataSource, columns: list[str]) -> None:
    """Validates that the columns exist in the dataset."""
    df_columns = data.source.load().columns.tolist()

    for column in columns:
        if column not in df_columns:
            logging.error("Column '%s' does not exist in the dataset.", column)
            raise ValueError(f"Column '{column}' does not exist in the dataset.")


def add_columns(columns: list[str]) -> None:
    """Updates the config file with the columns to include."""
    config = Config()
    config.content["include"] = columns
    config.write()


def preview_dataset(data: DataSource, config: Config) -> None:
    """Prints the first 10 rows of the dataset."""

    columns = config.content["include"]
    aggregations = config.content["aggregate"]

    df = data.source.load()
    df = df[columns]

    df = _apply_aggregations(df, aggregations)

    return df


def _apply_aggregations(
    df: pd.DataFrame, aggregations: list[dict[str, str]]
) -> pd.DataFrame:
    """Applies the aggregations to the dataframe."""
    for aggregation in aggregations:
        df_groupby = df.groupby(aggregation["grouped"])[aggregation["column"]]

        match aggregation["function"]:
            case "count":
                unmerged = df_groupby.size()
            case "distinct":
                # FIXME: this is not working
                pass
            case "sum":
                unmerged = df_groupby.sum()
            case "avg":
                unmerged = df_groupby.mean()
            case "max":
                unmerged = df_groupby.max()
            case "min":
                unmerged = df_groupby.min()
            case _:
                raise ValueError(f"Function {aggregation['function']} not supported.")
        unmerged = unmerged.reset_index(name=aggregation["alias"])
        df = df.merge(unmerged, on=aggregation["grouped"])

    return df


def apply() -> None:
    data = DataSource.get()
    config = Config()

    df = preview_dataset(data, config)

    DataSource.save(df, config)
