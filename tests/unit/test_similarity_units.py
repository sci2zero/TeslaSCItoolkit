"""tesci.similarity: the small helpers merge() is built from.

Also carries the fixture sanity check for tests/helpers/synthetic.py: for
every engineered case, the actual rapidfuzz ratio between its two values
is verified to land in the band the case expects. If a rapidfuzz upgrade
ever changes scoring, this is the test that should fail -- not one of the
merge integration tests, which would otherwise look like a merge-logic
regression instead of a fixture one.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from rapidfuzz import fuzz, utils

from tesci.similarity import (
    MergeState,
    _get_multi_stage_nums,
    _get_reference_column,
    _preprocess_data,
    assert_no_duplicate_columns,
    safe_cast_to_str,
    safe_replace,
    safe_truncate,
)
from tests.helpers import Bucket, synthetic_cases
from tests.helpers.synthetic import synthetic_similarity_columns


def test_safe_cast_to_str_converts_column():
    df = pd.DataFrame({"year": [2020, 2021]})

    safe_cast_to_str(df, "year")

    assert df["year"].tolist() == ["2020", "2021"]


def test_safe_cast_to_str_ignores_missing_column():
    df = pd.DataFrame({"year": [2020]})

    safe_cast_to_str(df, "not_there")

    assert list(df.columns) == ["year"]


def test_safe_replace_applies_mapping():
    df = pd.DataFrame({"doctype": ["Conference Paper", "Article"]})

    safe_replace(df, "doctype", {"Conference Paper": "Proceedings Paper"})

    assert df["doctype"].tolist() == ["Proceedings Paper", "Article"]


def test_safe_replace_ignores_missing_column():
    df = pd.DataFrame({"doctype": ["Article"]})

    safe_replace(df, "not_there", {"Article": "X"})


def test_safe_truncate_splits_on_marker():
    df = pd.DataFrame({"title": ["Main Title; Subtitle", "No Marker Here"]})

    safe_truncate(df, "title", ["; "])

    assert df["title"].tolist() == ["Main Title", "No Marker Here"]


def test_safe_truncate_ignores_missing_column():
    df = pd.DataFrame({"title": ["A; B"]})

    safe_truncate(df, "not_there", ["; "])


def test_preprocess_data_applies_replace_to_both_sides():
    df1 = pd.DataFrame({"doctype": ["Proceedings Paper"]})
    df2 = pd.DataFrame({"doctype": ["Conference Paper"]})

    _preprocess_data(df1, df2, "doctype", "doctype", {"replace": {"Conference Paper": "Proceedings Paper"}})

    assert df1["doctype"].iloc[0] == df2["doctype"].iloc[0] == "Proceedings Paper"


def test_preprocess_data_truncates_both_sides():
    df1 = pd.DataFrame({"into_title": ["Deep Learning Methods"]})
    df2 = pd.DataFrame({"from_title": ["Deep Learning Methods; A Comparative Study"]})

    _preprocess_data(df1, df2, "from_title", "into_title", {"truncate_after": ["; "]})

    assert df1["into_title"].iloc[0] == df2["from_title"].iloc[0] == "Deep Learning Methods"


def test_get_reference_column_finds_flagged_column():
    columns = [{"from_": "a", "into_": "a"}, {"from_": "b", "into_": "b", "is_reference": True}]

    ref = _get_reference_column(columns)

    assert ref["from_"] == "b"


def test_get_reference_column_returns_none_when_absent():
    assert _get_reference_column([{"from_": "a", "into_": "a"}]) is None


@pytest.mark.parametrize(
    ("stage_keys", "expected"),
    [
        ({"columns": []}, 1),
        ({"stage_1": {}}, 1),
        ({"stage_1": {}, "stage_2": {}}, 2),
        ({"stage_1": {}, "stage_2": {}, "stage_3": {}}, 3),
    ],
)
def test_get_multi_stage_nums(stage_keys, expected):
    class _Config:
        content = {"join": {"similarity_config": {"merge": stage_keys}}}

    assert _get_multi_stage_nums(_Config()) == expected


def test_assert_no_duplicate_columns_passes_when_unique():
    assert_no_duplicate_columns(["title", "authors"], ["article title", "authors"])


def test_assert_no_duplicate_columns_rejects_duplicate_from():
    with pytest.raises(ValueError, match="'from'"):
        assert_no_duplicate_columns(["title", "title"], ["a", "b"])


def test_assert_no_duplicate_columns_rejects_duplicate_into():
    with pytest.raises(ValueError, match="'into'"):
        assert_no_duplicate_columns(["a", "b"], ["title", "title"])


def _column_config_by_from(columns: list[dict]) -> dict[str, dict]:
    return {c["from_"]: c for c in columns}


@pytest.mark.parametrize("case", synthetic_cases(), ids=lambda c: c.name)
def test_synthetic_case_ratios_land_in_expected_band(case):
    """Verify each engineered row pair actually produces the ratio its name promises.

    This recomputes ratios the same way ``_merge_two_sources`` does
    (``fuzz.QRatio`` with ``utils.default_process``), directly against the
    raw field values -- it intentionally does not import or run any tesci
    merge code, so it stays a pure fixture check.
    """
    if case.wos_row is None or case.scopus_row is None:
        pytest.skip("orphan case has no pair of values to compare")

    columns = _column_config_by_from(synthetic_similarity_columns())
    worst = MergeState.EXACT
    severity = {MergeState.EXACT: 0, MergeState.SUGGESTED: 1, MergeState.POTENTIAL: 2, MergeState.NO_MATCH: 3}

    for from_col, config in columns.items():
        into_col = config["into_"]
        left = case.wos_row.get(from_col)
        right = case.scopus_row.get(into_col)
        if left is None or right is None or (isinstance(left, float) and math.isnan(left)):
            continue

        if config["similarity"]["preprocess"]:
            left, right = str(left), str(right)
            preprocess = config.get("preprocess")
            if preprocess and "truncate_after" in preprocess:
                for marker in preprocess["truncate_after"]:
                    left = left.split(marker)[0]
                    right = right.split(marker)[0]
            if preprocess and "replace" in preprocess:
                left = preprocess["replace"].get(left, left)
                right = preprocess["replace"].get(right, right)

        ratio = fuzz.QRatio(left, right, processor=utils.default_process)
        above, cutoff = config["similarity"]["above"], config["similarity"]["cutoff"]
        if ratio == 100:
            state = MergeState.EXACT
        elif ratio >= above:
            state = MergeState.SUGGESTED
        elif ratio >= cutoff:
            state = MergeState.POTENTIAL
        else:
            state = MergeState.NO_MATCH
        if severity[state] > severity[worst]:
            worst = state

    expected = {
        Bucket.EXACT: MergeState.EXACT,
        Bucket.SUGGESTED: MergeState.SUGGESTED,
        Bucket.POTENTIAL: MergeState.POTENTIAL,
        Bucket.NO_MATCH: MergeState.NO_MATCH,
    }[case.expected_bucket]
    assert worst == expected, (
        f"case '{case.name}' ({case.note}) computed worst-case bucket {worst}, expected {expected}"
    )
