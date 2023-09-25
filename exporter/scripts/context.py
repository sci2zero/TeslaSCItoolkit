import csv
import yaml
import pandas as pd
from pathlib import Path

# TODO: move over to separate .py

CONFIG_HOME = ".exporter"


class Data(object):
    """Base class for data sources."""

    def __init__(self, path: str) -> None:
        self.path = Path.cwd() / Path(CONFIG_HOME) / Path(path)
        # Check if the file exists
        if not self.path.exists():
            raise FileNotFoundError(f"File '{self.path}' not found.")

    def load(self):
        pass

    @property
    def get(self):
        pass


class CSV(Data):
    def __init__(self, path: str = None) -> None:
        super().__init__(path)

        self.df = self.load()

    def load(self):
        csv_file = open(self.path, "r")
        csv_reader = pd.read_csv(csv_file)
        # csv_reader = csv.DictReader(csv_file)
        return csv_reader


class DataSource(object):
    def __init__(self) -> None:
        self.config = Config()
        self.source = self.load()
        self._loaded = None

    @classmethod
    def get(cls):
        return cls()

    def load(self) -> Data:
        """Loads the data source."""
        source_path = self.config.content.get("data", None)
        if not source_path:
            raise ValueError("No data source found in the config file.")
        # TODO: auto-determine the data source type
        csv = CSV(source_path)
        self._loaded = list(csv.load())
        return csv


class Exporter(object):
    pass


class Include(object):
    pass


class Aggregate(object):
    pass


class Config(object):
    def __init__(self) -> None:
        self.config_path = Path.cwd() / Path(CONFIG_HOME) / "config.yml"
        self._content = self._read_config() or {}

    def _create_config(self):
        pass

    def _read_config(self):
        try:
            # Open and load the YAML file
            with open(self.config_path, "r") as yaml_file:
                return yaml.safe_load(yaml_file)
        except FileNotFoundError:
            print(f"File '{self.yaml_file_path}' not found.")
            raise
        except yaml.YAMLError as e:
            print(f"Error while loading YAML file: {e}")
            raise

    @classmethod
    def init(cls, dataset: str) -> None:
        """Stores the dataset name in the config file."""
        config = cls()
        config.content.setdefault("data", dataset)
        with open(config.config_path, "w") as yaml_file:
            yaml.dump(config.content, yaml_file, default_flow_style=False)

    @property
    def content(self):
        if not self._content:
            self._content = self._read_config()
        return self._content

    def write(self):
        with open(self.config_path, "w") as yaml_file:
            yaml.dump(self.content, yaml_file, default_flow_style=False)
