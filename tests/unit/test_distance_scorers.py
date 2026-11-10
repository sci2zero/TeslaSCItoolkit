"""The numeric and geo scorers a similarity column can use in place of QRatio."""

from __future__ import annotations

import pandas as pd
import pytest

from tesci.similarity import (
    MergeState,
    _column_state,
    column_distance,
    distance_state,
    haversine_km,
    validate_scorers,
)


def numeric_column(**similarity):
    return {"from_": "age", "into_": "cra", "similarity": {"scorer": "numeric", "within": 50, **similarity}}


def geo_column(**similarity):
    return {
        "from_": "lat",
        "into_": "latitude",
        "similarity": {
            "scorer": "geo",
            "within": 25,
            "longitude": {"from_": "long", "into_": "longitude"},
            **similarity,
        },
    }


def test_haversine_matches_a_known_distance():
    rome, milan = (41.9028, 12.4964), (45.4642, 9.19)
    assert haversine_km(*rome, *milan) == pytest.approx(477, abs=2)


def test_haversine_of_a_point_with_itself_is_zero():
    assert haversine_km(44.196, 8.32, 44.196, 8.32) == 0


@pytest.mark.parametrize(
    "distance, at_most, expected",
    [
        (0, None, MergeState.EXACT),
        (50, None, MergeState.SUGGESTED),
        (51, None, MergeState.POTENTIAL),
        (10_000, None, MergeState.POTENTIAL),
        (100, 100, MergeState.POTENTIAL),
        (101, 100, MergeState.NO_MATCH),
    ],
)
def test_distance_state_bands(distance, at_most, expected):
    assert distance_state(distance, {"within": 50, "at_most": at_most}) == expected


def test_within_zero_and_at_most_zero_demand_an_identical_value():
    similarity = {"within": 0, "at_most": 0}
    assert distance_state(0, similarity) == MergeState.EXACT
    assert distance_state(1, similarity) == MergeState.NO_MATCH


def test_numeric_distance_reads_numbers_written_as_text():
    """safe_cast_to_str may already have turned a numeric column into strings."""
    distance = column_distance(numeric_column(), pd.Series({"age": "3975"}), pd.Series({"cra": 3075.0}))
    assert distance == 900


def test_numeric_scorer_sees_the_digit_slip_that_qratio_misses():
    """QRatio rates 3075 against 3975 at 75, close enough to merge; 900 years apart is not."""
    state = _column_state(numeric_column(), pd.Series({"age": 3975}), pd.Series({"cra": 3075}))
    assert state == MergeState.POTENTIAL


def test_geo_distance_uses_both_coordinates():
    from_row = pd.Series({"lat": 45.756, "long": 13.696})
    into_row = pd.Series({"latitude": 44.196, "longitude": 8.32})
    assert column_distance(geo_column(), from_row, into_row) == pytest.approx(457, abs=1)


@pytest.mark.parametrize("blank", [None, float("nan"), "nan", "", "n/a"])
def test_a_blank_or_unreadable_value_is_not_judged(blank):
    assert _column_state(numeric_column(), pd.Series({"age": blank}), pd.Series({"cra": 3075})) is None
    from_row = pd.Series({"lat": 45.0, "long": blank})
    into_row = pd.Series({"latitude": 45.0, "longitude": 10.0})
    assert _column_state(geo_column(), from_row, into_row) is None


def test_qratio_remains_the_default_scorer():
    column = {"from_": "title", "into_": "title", "similarity": {"above": 90, "cutoff": 45}}
    state = _column_state(column, pd.Series({"title": "Edera"}), pd.Series({"title": "edera"}))
    assert state == MergeState.EXACT


def test_valid_scorer_settings_pass():
    validate_scorers([numeric_column(), geo_column(at_most=100)])


@pytest.mark.parametrize(
    "column, message",
    [
        (numeric_column(scorer="levenshtein"), "unknown scorer"),
        (numeric_column(within=None), '"within"'),
        (numeric_column(within=-1), '"within"'),
        (numeric_column(within=True), '"within"'),
        (numeric_column(at_most=10), '"at_most"'),
        ({**geo_column(), "similarity": {"scorer": "geo", "within": 25}}, '"longitude"'),
        ({**numeric_column(), "is_reference": True}, "reference column"),
    ],
)
def test_invalid_scorer_settings_are_rejected_with_the_column_named(column, message):
    with pytest.raises(ValueError, match=message) as error:
        validate_scorers([column])
    assert column["from_"] in str(error.value)
