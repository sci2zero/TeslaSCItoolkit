"""DataFrame comparisons that don't depend on column or row order.

``_apply_aggregations`` builds its output column list via
``list(set(...))`` (tesci/transformations.py), so column order is not
deterministic across runs/interpreters. Row order in the similarity merge
output likewise depends on iteration/sort order that isn't part of its
contract. Tests should compare on *content*, not incidental order.
"""

from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal


def column_set(df: pd.DataFrame) -> set[str]:
    return set(df.columns)


def assert_frame_equals_unordered(
    actual: pd.DataFrame, expected: pd.DataFrame, *, check_dtype: bool = True
) -> None:
    """Assert two frames hold the same data, ignoring column and row order."""
    assert column_set(actual) == column_set(
        expected
    ), f"columns differ: {column_set(actual)} != {column_set(expected)}"

    cols = sorted(expected.columns)
    left = actual[cols].sort_values(by=cols).reset_index(drop=True)
    right = expected[cols].sort_values(by=cols).reset_index(drop=True)
    assert_frame_equal(left, right, check_dtype=check_dtype, check_like=False)


def assert_same_rows(actual: pd.DataFrame, expected_rows: list[dict], *, subset: list[str]) -> None:
    """Assert ``actual[subset]`` contains exactly the rows in ``expected_rows`` (any order)."""
    expected = pd.DataFrame(expected_rows)[subset]
    assert_frame_equals_unordered(actual[subset], expected, check_dtype=False)
