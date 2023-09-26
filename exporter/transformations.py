import logging
import pandas as pd

from typing import Any
from exporter.scripts.context import DataSource, Config

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def aggregate(
    function: str, column: str | None, group: list[str] | None, alias: str | None
) -> None:
    data = DataSource.get()
    config = Config()
    validate_columns(data, [group])

    if "aggregate" not in config.content:
        config.content["aggregate"] = []

    config.content["aggregate"].append(
        {
            "function": function,
            "column": column if column else group[0],
            "grouped": group,
            "alias": alias if alias else f"{function}_{group[0]}",
        }
    )
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

    preview_dataset(data, config)


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

    print("Preview of the dataset:")

    df = data.source.load()
    df = df[columns]

    df = _apply_aggregations(df, aggregations)

    print(df.head(10))

    return df


def _apply_aggregations(
    df: pd.DataFrame, aggregations: list[dict[str, str]]
) -> pd.DataFrame:
    """Applies the aggregations to the dataframe."""
    for aggregation in aggregations:
        match aggregation["function"]:
            case "count":
                unmerged = (
                    df.groupby(aggregation["grouped"])
                    .size()
                    .reset_index(name=aggregation["alias"])
                )
            case "distinct":
                # FIXME: this is not working
                pass
            case "sum":
                unmerged = (
                    df.groupby(aggregation["grouped"])[aggregation["column"]]
                    .sum()
                    .reset_index(name=aggregation["alias"])
                )
            case "avg":
                unmerged = (
                    df.groupby(aggregation["grouped"])[aggregation["column"]]
                    .mean()
                    .reset_index(name=aggregation["alias"])
                )
            case "max":
                unmerged = (
                    df.groupby(aggregation["grouped"])[aggregation["column"]]
                    .max()
                    .reset_index(name=aggregation["alias"])
                )
            case "min":
                unmerged = (
                    df.groupby(aggregation["grouped"])[aggregation["column"]]
                    .min()
                    .reset_index(name=aggregation["alias"])
                )
        df = df.merge(unmerged, on=aggregation["grouped"])

    return df


def apply() -> None:
    data = DataSource.get()
    config = Config()

    df = preview_dataset(data, config)

    DataSource.save(df, config)
