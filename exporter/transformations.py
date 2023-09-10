import logging


from exporter.scripts.context import DataSource, Config

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def include(data: DataSource, columns: list[str]) -> None:
    """Columns to include in the final dataset"""
    validate_columns(data, columns)
    add_columns(columns)

    logging.info("Included columns: %s", columns)


def validate_columns(data: DataSource, columns: list[str]) -> None:
    """Validates that the columns exist in the dataset."""
    entry = list(data.source)[0]

    for column in columns:
        if column not in entry:
            logging.error("Column '%s' does not exist in the dataset.", column)
            raise ValueError(f"Column '{column}' does not exist in the dataset.")


def add_columns(columns: list[str]) -> None:
    """Updates the config file with the columns to include."""
    config = Config()
    config.content["include"] = columns
    config.write()
