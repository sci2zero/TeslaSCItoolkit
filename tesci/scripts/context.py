import os
from pathlib import Path

import attrs
import pandas as pd
import yaml

CONFIG_HOME = ".tesci"
CONFIG_NAME = os.environ.get("TESCI_CONFIG_NAME") or "config.yml"


class Data(object):
    """Base class for data sources."""

    def __init__(self, path: Path | str) -> None:
        if isinstance(path, Path):
            self.path = path
        else:
            self.path = Path.cwd() / Path(CONFIG_HOME) / Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"File '{self.path}' not found.")

    def load(self):
        pass

    def save(self):
        pass

    @property
    def get(self):
        pass


class Reader(Data):
    def __init__(self, path: str = None) -> None:
        super().__init__(path)

        self.df = self.load()

    def load(self) -> pd.DataFrame:
        match self.path.suffix:
            case ".xlsx" | ".xls":
                reader = pd.read_excel(self.path)
            case ".csv":
                reader = pd.read_csv(self.path)
            case _:
                raise ValueError(f"File type '{self.path.suffix}' not supported.")
        return reader


class DataSource(object):
    def __init__(self) -> None:
        self.join_sources = None
        self.config = Config()
        self.source: Data = self.load()
        self._loaded = None

    @classmethod
    def get(cls):
        return cls()

    def load(self) -> Data | list[Data]:
        """Loads the data source."""
        source_path = self.config.content.get("data", None).get("src", None)
        join_sources = self.config.content.get("join", None)
        if not source_path and not join_sources:
            raise ValueError("No data source found in the config file.")
        if source_path and join_sources:
            raise ValueError(
                "You cannot specify both a data source and join sources in the config file."
            )
        if join_sources.get("src", None) is not None:
            self.join_sources = JoinSources(
                sources=[
                    JoinSource(
                        Reader(join_source).load(),
                        join_sources.get("columns", None),
                    )
                    for join_source in join_sources["src"]
                ],
                how=join_sources.get("how", "left"),
                fuzzy=join_sources.get("fuzzy", False),
            )
            return self.join_sources
        else:
            reader = Reader(source_path)
            self._loaded = list(reader.load())
            return reader

    @classmethod
    def save_to_file(cls, df, config, name_override=None, path_override=None) -> None:
        """Saves the data source."""
        if name_override is None:
            dest_name = config.content.get("data", None).get("dest", None)
        else:
            dest_name = name_override
        if path_override is not None:
            dest_path = Path(path_override) / Path(dest_name)
        else:
            dest_path = Path.cwd() / Path(CONFIG_HOME) / Path(dest_name)
        if not dest_path:
            raise ValueError("No data destination found in the config file.")
        try:
            match dest_path.suffix:
                case ".xlsx" | ".xls":
                    df.to_excel(dest_path, index=False)
                case ".csv":
                    df.to_csv(dest_path, index=False)
                case _:
                    raise ValueError(f"File type '{dest_path.suffix}' not supported.")
        except Exception as e:
            print(f"Error while saving the data source: {e}")
            raise

    @classmethod
    def get_file_path(cls, config, name_override=None, path_override=None) -> Path:
        """Returns the path to the data source."""
        if name_override is None:
            dest_name = config.content.get("data", None).get("dest", None)
        else:
            dest_name = name_override
        if path_override is not None:
            dest_path = Path(path_override) / Path(dest_name)
        else:
            dest_path = Path.cwd() / Path(CONFIG_HOME) / Path(dest_name)
        return dest_path


class Config(object):
    def __init__(self) -> None:
        self.config_path = Path.cwd() / Path(CONFIG_HOME) / CONFIG_NAME
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

        if config.content is None:
            config.content = {"data": {"src": dataset, "dest": dest}}
        else:
            config.content["data"]["src"] = dataset
            config.content["data"]["dest"] = dest
        with open(config.config_path, "w") as yaml_file:
            yaml.safe_dump(config.content, yaml_file, default_flow_style=False)

    @property
    def dest_name(self) -> str:
        return self.content.get("data", None).get("dest", None)

    @property
    def destination_path(self) -> Path:
        dest_name = self.dest_name
        return Path.cwd() / Path(CONFIG_HOME) / Path(dest_name)

    @property
    def content(self):
        if not self._content:
            self._content = self._read_config()
        return self._content

    @content.setter
    def content(self, value):
        self._content = value

    def write(self):
        with open(self.config_path, "w") as yaml_file:
            yaml.safe_dump(self.content, yaml_file, default_flow_style=False)


class JoinSource(Data):
    def __init__(
        self,
        df: pd.DataFrame,
        columns: list[str] | None,
    ) -> None:
        self.df = df
        self.columns = columns


@attrs.define
class RatioConfig:
    above: float = attrs.field(default=None)
    cutoff: float = attrs.field(default=None)


@attrs.define
class MergeColumn:
    from_: str = attrs.field(default=None)
    into_: str = attrs.field(default=None)
    ratio: RatioConfig = attrs.field(default=None)


@attrs.define
class MergeConfig:
    columns: list[MergeColumn] = attrs.field(default=[])


@attrs.define
class FuzzyConfig:
    merge: MergeConfig = attrs.field(default=None)


@attrs.define
class JoinSources:
    sources: list[JoinSource] = attrs.field(default=[])
    how: str = attrs.field(default="left")
    fuzzy: bool = attrs.field(default=False)
    fuzzy_config: FuzzyConfig = attrs.field(default=None)
