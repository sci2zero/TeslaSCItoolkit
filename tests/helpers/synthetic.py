"""Synthetic citation records for exercising ``tesci.similarity.merge``.

Each :class:`SyntheticCase` below is one WoS-like row paired with one
Scopus-like row (or, for the ``orphan`` case, a Scopus-like row with no
counterpart), engineered so exactly one signal decides its
``MergeState`` bucket while everything else matches exactly. That keeps
merge tests readable ("this row differs only in publication year, so it
must land in no_matches") instead of every test re-deriving what a whole
row of fuzzy ratios adds up to.

The two frames intentionally use different column names on a few fields
(``Title``/``Article Title``, ``Year``/``Pub Year``) to mirror how real
citation exports never agree on a schema; the ``similarity_config`` in
``synthetic_similarity_columns()`` maps them the same way
``examples/04-config-similarity-join.yml`` maps WoS to Scopus fields.

Nothing here is fixed by contract with the production code except the
*bucket names* (``exact``/``suggested``/``potential``/``no_match``) that
``tesci.similarity.MergeState`` defines. The actual thresholds are chosen
for this module and verified against real rapidfuzz output by
``test_similarity_units.py::test_synthetic_case_ratios_land_in_expected_band``
so a rapidfuzz upgrade that changes scoring fails a single, obvious test
instead of quietly breaking every merge test.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pandas as pd

from tests.helpers.configs import merge_column

BASE_AUTHORS = "Jane Doe, John Smith"
BASE_YEAR = 2020
BASE_DOI = "10.1000/xyz123"
BASE_JOURNAL = "Journal of Applied Testing"
BASE_KEYWORDS = "alpha beta gamma delta"
BASE_DOCTYPE = "Article"


class Bucket(str, Enum):
    """Mirrors ``tesci.similarity.MergeState``, spelled the way output files are named."""

    EXACT = "exact"
    SUGGESTED = "suggested"
    POTENTIAL = "potential"
    NO_MATCH = "no"


@dataclass(frozen=True)
class SyntheticCase:
    """One engineered WoS/Scopus row pair (or an unmatched Scopus row)."""

    name: str
    expected_bucket: Bucket
    wos_row: dict | None
    scopus_row: dict | None
    note: str = ""


def _wos_row(title: str, **overrides) -> dict:
    row = {
        "Title": title,
        "Authors": BASE_AUTHORS,
        "Year": BASE_YEAR,
        "DOI": BASE_DOI,
        "Journal": BASE_JOURNAL,
        "Keywords": BASE_KEYWORDS,
        "DocType": BASE_DOCTYPE,
    }
    row.update(overrides)
    return row


def _scopus_row(title: str, **overrides) -> dict:
    row = {
        "Article Title": title,
        "Authors": BASE_AUTHORS,
        "Pub Year": BASE_YEAR,
        "DOI": BASE_DOI,
        "Journal": BASE_JOURNAL,
        "Keywords": BASE_KEYWORDS,
        "DocType": BASE_DOCTYPE,
    }
    row.update(overrides)
    return row


def synthetic_cases() -> list[SyntheticCase]:
    """The engineered row pairs, one per outcome tesci's merge can produce."""
    return [
        SyntheticCase(
            name="exact",
            expected_bucket=Bucket.EXACT,
            wos_row=_wos_row("Study of Alpha Particles in Layered Semiconductors"),
            scopus_row=_scopus_row("Study of Alpha Particles in Layered Semiconductors"),
            note="Every column matches verbatim.",
        ),
        SyntheticCase(
            name="suggested",
            expected_bucket=Bucket.SUGGESTED,
            wos_row=_wos_row(
                "Migration Patterns of Arctic Seabirds", Journal="Journal of Applied Field Testing"
            ),
            scopus_row=_scopus_row(
                "Migration Patterns of Arctic Seabirds", Journal="Journal of Applied Testing"
            ),
            note="Journal name gains one word; every other column is exact.",
        ),
        SyntheticCase(
            name="potential",
            expected_bucket=Bucket.POTENTIAL,
            wos_row=_wos_row("Thermal Conductivity of Amorphous Silicon Films", Keywords=BASE_KEYWORDS),
            scopus_row=_scopus_row(
                "Thermal Conductivity of Amorphous Silicon Films",
                Keywords="omega sigma tau upsilon",
            ),
            note="Keywords share no vocabulary; every other column is exact.",
        ),
        SyntheticCase(
            name="no_match",
            expected_bucket=Bucket.NO_MATCH,
            wos_row=_wos_row("Comparative Analysis of Urban Traffic Flow Models", Year=BASE_YEAR),
            scopus_row=_scopus_row(
                "Comparative Analysis of Urban Traffic Flow Models", **{"Pub Year": BASE_YEAR - 1}
            ),
            note="Year mismatch; the column's above=cutoff=100 forces NO_MATCH outright.",
        ),
        SyntheticCase(
            name="nan_doi",
            expected_bucket=Bucket.EXACT,
            wos_row=_wos_row("Optical Properties of Rare Earth Doped Glasses", DOI=None),
            scopus_row=_scopus_row("Optical Properties of Rare Earth Doped Glasses"),
            note="DOI missing on one side is skipped, not scored, so the row can still be EXACT.",
        ),
        SyntheticCase(
            name="preprocess",
            expected_bucket=Bucket.EXACT,
            wos_row=_wos_row(
                "Deep Learning Methods for Signal Denoising; A Comparative Study",
                DocType="Conference Paper",
            ),
            scopus_row=_scopus_row(
                "Deep Learning Methods for Signal Denoising", DocType="Proceedings Paper"
            ),
            note=(
                "Title needs truncate_after='; ' and DocType needs the "
                "'Conference Paper'->'Proceedings Paper' replace map to become EXACT."
            ),
        ),
        SyntheticCase(
            name="orphan",
            expected_bucket=Bucket.NO_MATCH,
            wos_row=None,
            scopus_row=_scopus_row(
                "Unrelated Marine Sediment Core Chronology",
                Authors="Unrelated Author",
                **{"Pub Year": 1980},
                DOI="10.9999/none",
            ),
            note="A Scopus-only row with no WoS counterpart; caught by the second (df1) pass.",
        ),
    ]


def wos_like_frame(cases: list[SyntheticCase] | None = None) -> pd.DataFrame:
    """The 'from_' side (WoS-shaped) of the synthetic cases, one row each (orphan excluded)."""
    cases = cases if cases is not None else synthetic_cases()
    return pd.DataFrame([c.wos_row for c in cases if c.wos_row is not None])


def scopus_like_frame(cases: list[SyntheticCase] | None = None) -> pd.DataFrame:
    """The 'into_' side (Scopus-shaped) of the synthetic cases, including the orphan row."""
    cases = cases if cases is not None else synthetic_cases()
    return pd.DataFrame([c.scopus_row for c in cases if c.scopus_row is not None])


def synthetic_similarity_columns() -> list[dict]:
    """The ``similarity_config.merge.columns`` matching :func:`synthetic_cases`."""
    return [
        merge_column(
            "Title",
            "Article Title",
            above=90,
            cutoff=50,
            is_reference=True,
            preprocess={"truncate_after": ["; "]},
        ),
        merge_column("Authors", "Authors", above=40, cutoff=20),
        merge_column("Year", "Pub Year", above=100, cutoff=100, cast_to_str=True),
        merge_column("DOI", "DOI", above=100, cutoff=100, cast_to_str=True),
        merge_column("Journal", "Journal", above=80, cutoff=60),
        merge_column("Keywords", "Keywords", above=95, cutoff=30),
        merge_column(
            "DocType",
            "DocType",
            above=100,
            cutoff=100,
            preprocess={"replace": {"Conference Paper": "Proceedings Paper"}},
        ),
    ]
