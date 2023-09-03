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
        self.config_path = Path(CONFIG_HOME)
        self._read_config()


    def _create_config(self):
        pass

    def _read_config(self):
        try:
            # Open and load the YAML file
            with open(self.config_path, 'r') as yaml_file:
                return yaml.safe_load(yaml_file)
        except FileNotFoundError:
            print(f"File '{self.yaml_file_path}' not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error while loading YAML file: {e}")
            return None
