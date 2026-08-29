"""Drive the shipped `examples/` configs through `tesci apply` end-to-end.

These are the closest thing tesci has to documentation of intended
behavior, so running them for real (against a temp project, not the
tracked `.tesci/`) is valuable independent of the more surgical unit
tests in test_aggregations.py.
"""

from __future__ import annotations

import shutil

import pandas as pd
import pytest


def _load_example(repo_root, tesci_project, config_name: str, data_name: str, data_subdir: str) -> None:
    shutil.copy(repo_root / "examples" / config_name, tesci_project.path("config.yml"))
    shutil.copy(repo_root / "examples" / data_subdir / data_name, tesci_project.path(data_name))


def test_example_02_adult_count_aggregate_and_sort(repo_root, tesci_project, ci_env):
    _load_example(repo_root, tesci_project, "02-config-adult.yml", "adult.csv", "adult")
    source = pd.read_csv(repo_root / "examples" / "adult" / "adult.csv")
    group_cols = ["hours-per-week", "education", "income"]
    alias = "count_hours-per-week_education_income"
    expected = (
        source.groupby(group_cols)
        .size()
        .reset_index(name=alias)
        .sort_values(by=[alias, "education", "income"], ascending=[True, True, False])
        .reset_index(drop=True)
    )

    result = tesci_project.run("apply")

    assert result.exit_code == 0, result.output
    actual = tesci_project.read_output("exported-adult.csv").reset_index(drop=True)
    assert actual[alias].tolist() == expected[alias].tolist()
    assert actual["education"].tolist() == expected["education"].tolist()
    assert actual["income"].tolist() == expected["income"].tolist()


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="examples/03 uses `grouped: age` as a bare string for 3 of its 5 aggregates; "
    "the grouped-column flatten iterates it character-by-character and KeyErrors "
    "(see test_aggregations.py::test_grouped_as_bare_string_is_supported).",
)
def test_example_03_multiple_aggregations(repo_root, tesci_project, ci_env):
    _load_example(repo_root, tesci_project, "03-config-multiple-aggregations.yml", "simple.csv", "simple")

    result = tesci_project.run("apply")

    assert result.exit_code == 0, result.output
