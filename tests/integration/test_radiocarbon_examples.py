"""Run examples/08-10 through the CLI on the bundled radiocarbon sample."""

from __future__ import annotations

import shutil

import pandas as pd
import pytest

SAMPLE = ("aida-sample.csv", "p3k14c-italy-sample.csv")


def _merge(repo_root, tesci_project, config_name):
    shutil.copy(repo_root / "examples" / config_name, tesci_project.path("config.yml"))
    sources = [repo_root / "examples" / "radiocarbon" / name for name in SAMPLE]
    result = tesci_project.run("similarity", "merge", "--src", str(sources[0]), "--src", str(sources[1]))
    assert result.exit_code == 0, result.output

    first, second = (pd.read_csv(path) for path in sources)
    pairs = pd.read_csv(tesci_project.path("config-final-pairs.csv"))
    pairs = pairs.drop_duplicates(["first_index", "second_index"])
    pairs["aida"] = pairs.first_index.map(first.LabID).fillna("(blank)") + " " + pairs.first_index.map(first.CRA).astype(str)
    pairs["p3k14c"] = pairs.second_index.map(second.LabID)
    return dict(zip(pairs.aida + " / " + pairs.p3k14c, pairs.bucket))


def test_lab_code_alone_pairs_case_variants_and_both_r98_rows(repo_root, tesci_project):
    buckets = _merge(repo_root, tesci_project, "08-config-radiocarbon-lab-code.yml")

    assert buckets["GrA-19820 6390 / GRA-19820"] == "exact"
    assert buckets["R-98 3975 / R-98"] == "exact"
    assert buckets["R-98 3075 / R-98"] == "exact"
    assert buckets["I-15466 2880 / L-15466"] == "no_match"
    assert not any(pair.startswith("(blank)") for pair in buckets)


@pytest.mark.parametrize(
    "pair, bucket",
    [
        ("AA-64825 4476 / AA-64825", "exact"),
        ("A-3951 3760 / A-3951", "suggested"),
        ("R-98 3975 / R-98", "suggested"),
        ("R-98 3075 / R-98", "potential"),
        ("A-4472 4840 / A-4472", "potential"),
        ("GrA-19820 6390 / GRA-19820", "potential"),
        ("R-2702 6447 / R-2702", "potential"),
        ("R-398 8990 / R-398", "potential"),
        ("LTL-201a 6261 / LTL-201A", "potential"),
    ],
)
def test_field_conflicts_keep_disagreements_for_review(repo_root, tesci_project, pair, bucket):
    assert _merge(repo_root, tesci_project, "09-config-radiocarbon-field-conflicts.yml")[pair] == bucket


def test_code_variants_with_identical_measurements_are_potential(repo_root, tesci_project):
    buckets = _merge(repo_root, tesci_project, "10-config-radiocarbon-lab-code-typos.yml")

    assert buckets["I-15466 2880 / L-15466"] == "potential"
    assert buckets["Lyon-4043 2145 / LY-4043"] == "potential"
    assert buckets["R-98 3075 / R-98"] == "no_match"
    assert buckets["R-98 3975 / R-98"] == "exact"
