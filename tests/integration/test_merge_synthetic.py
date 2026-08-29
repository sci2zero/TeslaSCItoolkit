"""tesci.similarity._merge_two_sources against engineered synthetic rows.

This drives the actual bucket-classification algorithm (rapidfuzz scoring,
worst-state-wins, the two-pass df2-then-df1 reconciliation) through its
real entry point, with just enough isolation to make it testable:

* ``config`` is a bare object exposing ``.content`` -- ``_merge_two_sources``
  never constructs its own ``Config()`` for the merge settings, only for
  *saving* output files (``DataSource.save_to_file(df, Config(), ...)``,
  see similarity.py), which is why ``tesci_project`` (giving that inner
  ``Config()`` a config.yml to read) is still needed even though the merge
  settings themselves come from the fake ``config`` object below.
  ``captured_outputs`` just avoids round-tripping bucket frames through
  disk for every assertion (see conftest.py).
* ``stage=None`` is deliberate: it's the single-stage code path
  (``similarity_config.merge.columns`` with no ``stage_N`` wrapper, as in
  examples/04-config-similarity-join.yml). The public ``similarity.merge()``
  entry point always passes ``stage=1`` now, which makes this branch
  unreachable from the CLI -- see test_merge_multistage.py for that bug.
  Testing the classification logic here, independent of that orchestration
  bug, is what keeps these tests meaningful across the planned rewrite.
"""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import tesci.similarity as similarity
from tests.helpers import Bucket, similarity_config, synthetic_cases
from tests.helpers.synthetic import scopus_like_frame, synthetic_similarity_columns, wos_like_frame


def _run_merge(tesci_project, captured_outputs, *, drop_duplicates: bool = False):
    tesci_project.write_config({})
    wos_path = tesci_project.add_csv("wos.csv", wos_like_frame())
    scopus_path = tesci_project.add_csv("scopus.csv", scopus_like_frame())
    config = SimpleNamespace(
        content=similarity_config(synthetic_similarity_columns(), drop_duplicates=drop_duplicates)
    )

    similarity._merge_two_sources(
        scopus_path, wos_path, config, stage=None, save_to_disk_name_override=None, dest=None
    )
    return captured_outputs


def _bucket_file(bucket: Bucket) -> str:
    return f"config-{bucket.value}-matches.xls"


PAIRED_CASES = [c for c in synthetic_cases() if c.wos_row is not None and c.scopus_row is not None]


@pytest.mark.parametrize("case", PAIRED_CASES, ids=lambda c: c.name)
def test_matched_row_lands_in_its_expected_bucket(tesci_project, captured_outputs, case):
    saved = _run_merge(tesci_project, captured_outputs)

    bucket_df = saved[_bucket_file(case.expected_bucket)]
    title = case.scopus_row["Article Title"]
    assert title in bucket_df["article title"].values, (
        f"expected '{case.name}' ({case.note}) in {_bucket_file(case.expected_bucket)}, "
        f"got buckets: { {k: v['article title'].tolist() for k, v in saved.items() if 'article title' in v.columns} }"
    )


def test_orphan_row_is_caught_by_the_second_pass(tesci_project, captured_outputs):
    saved = _run_merge(tesci_project, captured_outputs)

    orphan = next(c for c in synthetic_cases() if c.name == "orphan")
    no_matches = saved[_bucket_file(Bucket.NO_MATCH)]
    assert orphan.scopus_row["Article Title"] in no_matches["article title"].values


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="_merge_two_sources computes merged_df (the actual union of both sides' fields, "
    "e.g. carrying WoS's unique 'title'/'year' onto the Scopus schema) only to print analytics "
    "from it -- it is never passed to save_to_file. 'config-final.xls' is just the four "
    "match-bucket dataframes concatenated (each already renamed to the into_ schema), so no "
    "saved output ever contains the merged record.",
)
def test_final_output_contains_the_merged_record_with_both_sides_fields(tesci_project, captured_outputs):
    saved = _run_merge(tesci_project, captured_outputs)

    final_df = saved["config-final.xls"]
    assert "title" in final_df.columns


def test_drop_duplicates_removes_duplicate_rows_before_matching(tesci_project, captured_outputs):
    tesci_project.write_config({})
    wos = wos_like_frame()
    wos_with_dupe = pd.concat([wos, wos.iloc[[0]]], ignore_index=True)
    wos_path = tesci_project.add_csv("wos.csv", wos_with_dupe)
    scopus_path = tesci_project.add_csv("scopus.csv", scopus_like_frame())
    config = SimpleNamespace(
        content=similarity_config(synthetic_similarity_columns(), drop_duplicates=True)
    )

    similarity._merge_two_sources(
        scopus_path, wos_path, config, stage=None, save_to_disk_name_override=None, dest=None
    )

    exact_case = next(c for c in synthetic_cases() if c.name == "exact")
    exact_bucket = captured_outputs[_bucket_file(Bucket.EXACT)]
    matches = exact_bucket[exact_bucket["article title"] == exact_case.scopus_row["Article Title"]]
    assert len(matches) == 1, "the duplicated WoS row should have been dropped before matching"


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="no_matches_df = pd.concat([no_matches_df, potential_matches_df]) means every "
    "potential match is also written into the no-matches file, double counting it.",
)
def test_no_matches_bucket_does_not_duplicate_potential_matches(tesci_project, captured_outputs):
    saved = _run_merge(tesci_project, captured_outputs)

    potential_case = next(c for c in synthetic_cases() if c.name == "potential")
    no_matches = saved[_bucket_file(Bucket.NO_MATCH)]
    assert potential_case.scopus_row["Article Title"] not in no_matches["article title"].values
