"""Properties the merge algorithm should hold regardless of implementation details.

These don't pin down exact row counts or bucket assignments (that's
test_merge_synthetic.py); they check broader guarantees that should
survive a rewrite: every input row is accounted for somewhere, running
twice gives the same answer, and which physical file is "first" doesn't
change the outcome.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import tesci
import tesci.similarity as similarity
from tests.helpers import assert_frame_equals_unordered, merge_column, similarity_config, synthetic_cases
from tests.helpers.synthetic import scopus_like_frame, synthetic_similarity_columns, wos_like_frame

BUCKET_FILES = [
    "config-exact-matches.xls",
    "config-suggested-matches.xls",
    "config-potential-matches.xls",
    "config-no-matches.xls",
]


def _run_merge(tesci_project, captured_outputs):
    tesci_project.write_config({})
    wos_path = tesci_project.add_csv("wos.csv", wos_like_frame())
    scopus_path = tesci_project.add_csv("scopus.csv", scopus_like_frame())
    config = SimpleNamespace(content=similarity_config(synthetic_similarity_columns()))
    similarity._merge_two_sources(
        scopus_path, wos_path, config, stage=None, save_to_disk_name_override=None, dest=None
    )
    return captured_outputs


def test_every_row_appears_in_some_bucket(tesci_project, captured_outputs):
    saved = _run_merge(tesci_project, captured_outputs)

    seen_titles: set[str] = set()
    for name in BUCKET_FILES:
        df = saved[name]
        if "article title" in df.columns:
            seen_titles |= set(df["article title"].dropna())

    expected_titles = {c.scopus_row["Article Title"] for c in synthetic_cases() if c.scopus_row is not None}
    missing = expected_titles - seen_titles
    assert not missing, f"rows missing from every output bucket: {missing}"


def test_merge_is_deterministic(tesci_project, captured_outputs):
    first = dict(_run_merge(tesci_project, captured_outputs))

    captured_outputs.clear()
    second = _run_merge(tesci_project, captured_outputs)

    assert set(first) == set(second)
    for name in BUCKET_FILES:
        assert_frame_equals_unordered(first[name], second[name], check_dtype=False)


def test_bucket_membership_is_symmetric_in_source_order(tesci_project, captured_outputs):
    """Swapping which file is 'first_src' vs 'second_src' shouldn't change who matches whom.

    Uses a minimal identity-mapped single-column config (both sides named
    'title') so the from_/into_ config doesn't itself have to be swapped
    along with the physical files.
    """
    tesci_project.write_config({})
    x_path = tesci_project.add_csv("x.csv", [{"title": "Alpha Study"}, {"title": "Beta Study"}])
    y_path = tesci_project.add_csv("y.csv", [{"title": "Alpha Study"}, {"title": "Gamma Study"}])
    columns = [merge_column("title", "title", above=90, cutoff=50, is_reference=True)]

    config_xy = SimpleNamespace(content=similarity_config(columns))
    similarity._merge_two_sources(
        x_path, y_path, config_xy, stage=None, save_to_disk_name_override="xy-final.xls", dest=None
    )
    xy_matched = set(captured_outputs["config-exact-matches.xls"]["title"])

    captured_outputs.clear()
    columns_swapped = [merge_column("title", "title", above=90, cutoff=50, is_reference=True)]
    config_yx = SimpleNamespace(content=similarity_config(columns_swapped))
    similarity._merge_two_sources(
        y_path, x_path, config_yx, stage=None, save_to_disk_name_override="yx-final.xls", dest=None
    )
    yx_matched = set(captured_outputs["config-exact-matches.xls"]["title"])

    assert xy_matched == yx_matched == {"Alpha Study"}


@pytest.mark.known_bug
@pytest.mark.xfail(strict=True, reason="tesci/transformations.py:_apply_join_fuzzy has a live breakpoint()")
def test_shipped_package_has_no_leftover_breakpoints():
    package_dir = Path(tesci.__file__).parent
    offenders = [
        p for p in package_dir.rglob("*.py") if "breakpoint(" in p.read_text(encoding="utf-8")
    ]
    assert not offenders, f"breakpoint() left in shipped code: {offenders}"
