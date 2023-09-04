import pandas as pd
import yaml

from pathlib import Path

 
CONFIG_HOME = ".exporter"

class CSV(object):
    def __init__(self, home: str = None, debug: bool = False) -> None:
        self.path = Path(home)
         # Check if the file exists
        if not self.path.exists():
            raise FileNotFoundError(f"File '{self.path}' not found.")

        self.debug = debug
        self.df = self._open()

    def _open(self):
        try:
            return pd.read_csv(self.path)
        except Exception as e:
            if self.debug:
                print(f"An error occurred: {str(e)}")
            return None
    
        
class Exporter(object):
    pass

class Include(object):
    pass

class Aggregate(object):
    pass

class Config(object):
    
    def __init__(self) -> None:
        self.config_path = Path.cwd() / Path(CONFIG_HOME) / "config.yml"
        self.content = self._read_config() or {}


    def _create_config(self):
        pass

    def _read_config(self):
        try:
            # Open and load the YAML file
            with open(self.config_path, 'r') as yaml_file:
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
        with open(config.config_path, 'w') as yaml_file:
            yaml.dump(config.content, yaml_file, default_flow_style=False)
