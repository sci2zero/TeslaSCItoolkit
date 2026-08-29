"""tesci.transformations: _apply_aggregations, _apply_sort, _apply_include.

Expected values are computed with plain pandas inside each test rather
than hardcoded, so a test failure means the aggregation engine disagrees
with pandas semantics, not that a golden number went stale.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tesci.transformations import _apply_aggregations, _apply_include, _apply_sort
from tests.helpers import aggregate_entry, assert_frame_equals_unordered

PEOPLE = pd.DataFrame(
    [
        {"id": 1, "name": "John", "age": 30, "salary": 100},
        {"id": 2, "name": "Jane", "age": 30, "salary": 200},
        {"id": 3, "name": "Bob", "age": 40, "salary": 150},
        {"id": 4, "name": "Amy", "age": 40, "salary": 50},
        {"id": 5, "name": "Tom", "age": 30, "salary": 300},
    ]
)


def people() -> pd.DataFrame:
    return PEOPLE.copy()


@pytest.mark.parametrize(
    ("function", "expected"),
    [
        ("sum", PEOPLE["salary"].sum()),
        ("avg", PEOPLE["salary"].mean()),
        ("max", PEOPLE["salary"].max()),
        ("min", PEOPLE["salary"].min()),
    ],
)
def test_ungrouped_aggregation(function, expected):
    aggregation = aggregate_entry("result", ["salary"], function)

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert list(out.columns) == ["result"]
    assert out["result"].iloc[0] == pytest.approx(expected)


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="the 'count' case calls df_aggregate.size() to mirror the grouped path's "
    "GroupBy.size(), but an ungrouped df_aggregate is a plain Series/DataFrame whose "
    "'.size' is an int attribute, not a method -- TypeError: 'int' object is not callable.",
)
def test_ungrouped_count_counts_rows():
    aggregation = aggregate_entry("result", ["salary"], "count")

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert out["result"].iloc[0] == len(PEOPLE)


def test_grouped_by_single_column():
    aggregation = aggregate_entry("sum_salary_by_age", ["salary"], "sum", grouped=["age"])
    expected = PEOPLE.groupby("age")["salary"].sum().reset_index(name="sum_salary_by_age")

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert_frame_equals_unordered(out, expected, check_dtype=False)


def test_grouped_by_multiple_columns():
    aggregation = aggregate_entry("count_by_age_salary", ["name"], "count", grouped=["age", "salary"])
    expected = (
        PEOPLE.groupby(["age", "salary"])["name"]
        .size()
        .reset_index(name="count_by_age_salary")
    )

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert_frame_equals_unordered(out, expected, check_dtype=False)


def test_multiple_aggregations_are_merged_onto_one_frame():
    aggregations = [
        aggregate_entry("sum_salary_by_age", ["salary"], "sum", grouped=["age"]),
        aggregate_entry("max_salary_by_age", ["salary"], "max", grouped=["age"]),
    ]
    expected = PEOPLE.groupby("age")["salary"].agg(sum_salary_by_age="sum", max_salary_by_age="max").reset_index()

    out = _apply_aggregations(people(), aggregations, columns=None)

    assert_frame_equals_unordered(out, expected, check_dtype=False)


def test_grouped_output_is_deduplicated():
    aggregation = aggregate_entry("sum_salary_by_age", ["salary"], "sum", grouped=["age"])

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert len(out) == PEOPLE["age"].nunique()


def test_unsupported_function_raises_value_error():
    aggregation = aggregate_entry("result", ["salary"], "median")

    with pytest.raises(ValueError, match="not supported"):
        _apply_aggregations(people(), [aggregation], columns=None)


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="grouped-column flatten assumes 'grouped' is always a list; a bare string "
    "(examples/03-config-multiple-aggregations.yml uses `grouped: age`) is iterated "
    "character-by-character instead.",
)
def test_grouped_as_bare_string_is_supported():
    aggregation = aggregate_entry("sum_salary_by_age", ["salary"], "sum", grouped="age")
    expected = PEOPLE.groupby("age")["salary"].sum().reset_index(name="sum_salary_by_age")

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert_frame_equals_unordered(out, expected, check_dtype=False)


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="'distinct' is advertised by the CLI (AGGREGATE_FUNCTIONS) but the engine's "
    "match statement has no case for it.",
)
def test_distinct_function_is_supported():
    aggregation = aggregate_entry("distinct_ages", ["age"], "distinct")

    out = _apply_aggregations(people(), [aggregation], columns=None)

    assert out["distinct_ages"].iloc[0] == PEOPLE["age"].nunique()


def test_apply_sort_mixed_ascending_and_descending():
    sort = {"ascending": ["age"], "descending": ["salary"]}

    out = _apply_sort(people(), sort)

    expected = PEOPLE.sort_values(by=["age", "salary"], ascending=[True, False])
    pd.testing.assert_frame_equal(out.reset_index(drop=True), expected.reset_index(drop=True))


def test_apply_sort_none_is_a_no_op():
    out = _apply_sort(people(), None)

    pd.testing.assert_frame_equal(out, PEOPLE)


def test_apply_include_projects_columns():
    out = _apply_include(people(), ["name", "age"])

    assert list(out.columns) == ["name", "age"]
    assert len(out) == len(PEOPLE)


def test_apply_include_unknown_column_raises():
    with pytest.raises(KeyError):
        _apply_include(people(), ["not_a_column"])
