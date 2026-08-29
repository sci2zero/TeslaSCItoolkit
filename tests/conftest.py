from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests.helpers.project import TesciProject

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """The tesci repository root, for reading real example configs/fixtures."""
    return REPO_ROOT


@pytest.fixture
def tesci_project(tmp_path, monkeypatch) -> TesciProject:
    """A fresh ``.tesci`` project in an isolated temp dir, chdir'd into.

    tesci resolves config/data paths off ``Path.cwd()``
    (``tesci.scripts.context.Config``/``DataSource``), so every test that
    touches config or data files needs this rather than the real repo's
    ``.tesci/`` directory.
    """
    monkeypatch.chdir(tmp_path)
    return TesciProject(tmp_path)


@pytest.fixture
def ci_env(monkeypatch):
    """Set TESCI_RUN_FROM_CI so `tesci apply` skips its interactive confirm prompt."""
    monkeypatch.setenv("TESCI_RUN_FROM_CI", "1")


@pytest.fixture
def captured_outputs(monkeypatch) -> dict[str, pd.DataFrame]:
    """Record every DataFrame ``DataSource.save_to_file`` is asked to save.

    Real ``.xls`` writes do work here (see
    ``test_config.py::test_data_source_saves_xls_on_current_pandas_stack``
    for why that's less obvious than it sounds) -- this fixture is purely a
    convenience so merge tests can assert on results in memory instead of
    round-tripping every bucket through disk on every test.
    """
    from tesci.scripts.context import DataSource

    saved: dict[str, pd.DataFrame] = {}

    def fake_save_to_file(cls, df, config, name_override=None, path_override=None):
        dest_path = DataSource.get_file_path(
            config, name_override=name_override, path_override=path_override
        )
        saved[dest_path.name] = df.copy()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = dest_path.suffix.lower()
        if suffix in (".xls", ".xlsx"):
            df.to_excel(dest_path, index=False, engine="openpyxl")
        elif suffix == ".csv":
            df.to_csv(dest_path, index=False)
        else:
            raise ValueError(f"File type '{suffix}' not supported.")

    monkeypatch.setattr(DataSource, "save_to_file", classmethod(fake_save_to_file))
    return saved
