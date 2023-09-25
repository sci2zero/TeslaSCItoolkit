import logging
import pandas as pd

from exporter.scripts.context import DataSource, Config

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def aggregate(type: str, group: list[str], alias: str) -> None:
    data = DataSource.get()
    config = Config()
    validate_columns(data, [group])

    if "aggregate" not in config.content:
        config.content["aggregate"] = []

    config.content["aggregate"].append(
        {
            "type": type,
            "grouped": group,
            "alias": alias if alias else f"{type}_{group[0]}",
        }
    )
    config.write()


def preview() -> None:
    """Preview the changes that will be applied to the dataset"""
    data = DataSource.get()
    config = Config()

    if "include" not in config.content:
        logging.info("No columns to include in the dataset.")
        return

    columns = config.content["include"]
    validate_columns(data, columns)
    logging.info("Included columns: %s", columns)

    aggregations = config.content["aggregate"]
    logging.info("Aggregations to apply: %s", aggregations)

    preview_dataset(data, columns, aggregations)


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


def preview_dataset(
    data: DataSource, columns: list[str], aggregations: list[dict[str]]
) -> None:
    """Prints the first 10 rows of the dataset."""
    print("Preview of the dataset:")

    df = data.source.load()
    df = df[columns]

    breakpoint()
    for aggregation in aggregations:
        match aggregation["type"]:
            case "count":
                unmerged = (
                    df.groupby(aggregation["grouped"])
                    .size()
                    .reset_index(name=aggregation["alias"])
                )
            case "distinct":
                # TODO: fix this
                pass
            case "max":
                # TODO: fix this
                pass
            case "min":
                # TODO: fix this
                pass
            case "sum":
                # TODO: fix this
                pass
            case "avg":
                # TODO: fix this
                pass
            case _:
                raise ValueError(f"Invalid aggregation type: {aggregation['type']}")

        df = df.merge(unmerged, on=aggregation["grouped"])

    print(df.head(10))


def apply() -> None:
    pass
