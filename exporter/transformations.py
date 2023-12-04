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

    if (
        "aggregate" not in config.content
        and "include" not in config.content
        and "join" not in config.content
    ):
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
    sort = config.content.get("sort")

    df = _apply_join(data) if data.join_sources is not None else data.source.load()
    df = _apply_include(df, columns)
    df = _apply_aggregations(df, aggregations, columns)
    df = _apply_sort(df, sort)

    return df


def _apply_aggregations(
    df: pd.DataFrame, aggregations: list[dict[str, str]], columns: list[str]
) -> pd.DataFrame:
    """Applies the aggregations to the dataframe."""
    if aggregations is None:
        return df

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


def _apply_join(data: DataSource) -> pd.DataFrame:
    """Applies the join to the dataframe."""
    df = None
    for join_source in data.join_sources:
        current_df = join_source.df
        if df is None:
            df = current_df
            continue
        df = df.merge(current_df, on=join_source.columns, how=join_source.how)
    return df


def _apply_include(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Applies the include to the dataframe."""
    if columns is None:
        return df

    return df[columns]


def _apply_sort(df: pd.DataFrame, sort: dict[str, Any]) -> pd.DataFrame:
    if sort is None:
        return df

    sort_columns = []
    is_ascending = []

    for strategy, columns in sort.items():
        match strategy:
            case "ascending":
                sort_columns.extend(columns)
                is_ascending.extend([True] * len(columns))
            case "descending":
                sort_columns.extend(columns)
                is_ascending.extend([False] * len(columns))

    return df.sort_values(by=sort_columns, ascending=is_ascending)


def apply() -> None:
    data = DataSource.get()
    config = Config()

    df = preview_dataset(data, config)

    DataSource.save_to_file(df, config)
