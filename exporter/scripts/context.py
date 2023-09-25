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

    def save(self):
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
        source_path = self.config.content.get("data", None).get("src", None)
        if not source_path:
            raise ValueError("No data source found in the config file.")
        # TODO: auto-determine the data source type
        csv = CSV(source_path)
        self._loaded = list(csv.load())
        return csv

    @classmethod
    def save(cls, df, config) -> None:
        """Saves the data source."""
        dest_name = config.content.get("data", None).get("dest", None)
        dest_path = Path.cwd() / Path(CONFIG_HOME) / Path(dest_name)

        if not dest_path:
            raise ValueError("No data destination found in the config file.")
        try:
            df.to_csv(dest_path, index=False)
        except Exception as e:
            print(f"Error while saving the data source: {e}")
            raise


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
    def init(cls, dataset: str, dest: str) -> None:
        """Stores the dataset name in the config file."""
        config = cls()

        if config.content.get("data", None) is None:
            config.content.setdefault("data", {"src": dataset, "dest": dest})
        else:
            config.content["data"]["src"] = dataset
            config.content["data"]["dest"] = dest
        with open(config.config_path, "w") as yaml_file:
            yaml.safe_dump(config.content, yaml_file, default_flow_style=False)

    @property
    def content(self):
        if not self._content:
            self._content = self._read_config()
        return self._content

    def write(self):
        with open(self.config_path, "w") as yaml_file:
            yaml.safe_dump(self.content, yaml_file, default_flow_style=False)
