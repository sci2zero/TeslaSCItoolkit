"""A temporary ``.tesci`` workspace, wired the way tesci expects.

tesci resolves configuration and data files relative to ``Path.cwd() / ".tesci"``
(see ``tesci.scripts.context.Config``/``DataSource``). ``TesciProject`` is the
single place that knows this, so tests never chdir or build paths by hand.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from click.testing import CliRunner, Result

CONFIG_HOME = ".tesci"


class TesciProject:
    """A tmp_path-backed ``.tesci`` project.

    Created by the ``tesci_project`` fixture, which also chdirs the test
    process into ``root`` for the duration of the test.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.tesci_dir = root / CONFIG_HOME
        self.tesci_dir.mkdir(parents=True, exist_ok=True)

    def path(self, name: str) -> Path:
        """Resolve ``name`` inside the ``.tesci`` directory."""
        return self.tesci_dir / name

    def write_config(self, content: dict[str, Any], name: str = "config.yml") -> Path:
        """Write a config dict as YAML, the way a hand-edited config.yml looks."""
        dest = self.path(name)
        with open(dest, "w") as fh:
            yaml.safe_dump(content, fh, default_flow_style=False)
        return dest

    def read_config(self, name: str = "config.yml") -> dict[str, Any]:
        with open(self.path(name), "r") as fh:
            return yaml.safe_load(fh)

    def add_csv(self, name: str, data: pd.DataFrame | list[dict[str, Any]]) -> Path:
        """Write ``data`` as a CSV fixture under ``.tesci/<name>``."""
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        dest = self.path(name)
        df.to_csv(dest, index=False)
        return dest

    def add_excel(self, name: str, data: pd.DataFrame | list[dict[str, Any]]) -> Path:
        """Write ``data`` as an Excel fixture, always via the openpyxl engine."""
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        dest = self.path(name)
        df.to_excel(dest, index=False, engine="openpyxl")
        return dest

    def read_output(self, name: str) -> pd.DataFrame:
        """Read back a file tesci wrote into ``.tesci/<name>``."""
        dest = self.path(name)
        suffix = dest.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(dest)
        if suffix in (".xls", ".xlsx"):
            return pd.read_excel(dest)
        raise ValueError(f"Don't know how to read output file '{name}'.")

    def exists(self, name: str) -> bool:
        return self.path(name).exists()

    def run(self, *args: str, input: str | None = None) -> Result:
        """Invoke the ``tesci`` CLI in-process against this project.

        Import is deferred so ``tesci_project`` alone doesn't require the
        click command tree to already be importable/registered.
        """
        from tesci.scripts.tesci import cli

        runner = CliRunner()
        return runner.invoke(cli, list(args), input=input)
