import logging
import pandas as pd
import attrs

from typing import Any
from exporter.scripts.context import DataSource, Config
from exporter.types import Aggregate

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def aggregate(
    function: str, columns: list[str] | None, group: list[str] | None, alias: str | None
) -> None:
    data = DataSource.get()
    config = Config()

    if group is not None:
        validate_columns(data, group)

    if "aggregate" not in config.content:
        config.content["aggregate"] = []

    aggregate_dict = attrs.asdict(Aggregate(function, columns, alias))

    if group is not None:
        aggregate_dict["grouped"] = group

    config.content["aggregate"].append(aggregate_dict)
    config.write()


def preview() -> Any:
    """Preview the changes that will be applied to the dataset"""
    data = DataSource.get()
    config = Config()

    if not "aggregate" in config.content and not "include" in config.content:
        logging.info("No transformations to apply to the dataset.")
        return

    if "include" in config.content:
        columns = config.content["include"]
        validate_columns(data, columns)
        logging.info("Included columns: %s", columns)

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
    """Applies the transformations of the dataframe."""

    columns = config.content.get("include")
    aggregations = config.content.get("aggregate")

    df = data.source.load()

    df = df[columns] if columns is not None else df

    df = _apply_aggregations(df, aggregations, columns)

    return df


def _apply_aggregations(
    df: pd.DataFrame, aggregations: list[dict[str, str]], columns: list[str]
) -> pd.DataFrame:
    """Applies the aggregations to the dataframe."""
    data = {}
    for aggregation in aggregations:
        aggregation["columns"] = (
            aggregation["columns"][0]
            if len(aggregation["columns"]) == 1
            else aggregation["columns"]
        )
        if aggregation.get("grouped") is not None:
            df_aggregate = df.groupby(aggregation["grouped"])[aggregation["columns"]]
        else:
            # without groupby, expected aggregate function is applied to a single column
            df_aggregate = df[aggregation["columns"]]

        match aggregation["function"]:
            case "count":
                unmerged = df_aggregate.size()
            case "sum":
                unmerged = df_aggregate.sum()
            case "avg":
                unmerged = df_aggregate.mean()
            case "max":
                unmerged = df_aggregate.max()
            case "min":
                unmerged = df_aggregate.min()
            case _:
                raise ValueError(f"Function {aggregation['function']} not supported.")

        if aggregation.get("grouped") is not None:
            unmerged = unmerged.reset_index(name=aggregation["alias"])
            df = df.merge(unmerged, on=aggregation["grouped"])
        elif columns is not None:
            df[aggregation["alias"]] = unmerged
        else:
            # no columns included, create new dataframe with aggregated data
            data[aggregation["alias"]] = [unmerged]

    if columns is None and data != {}:
        df = pd.DataFrame(data)

    df = df.drop_duplicates()

    return df


def apply() -> None:
    data = DataSource.get()
    config = Config()

    df = preview_dataset(data, config)

    DataSource.save(df, config)
