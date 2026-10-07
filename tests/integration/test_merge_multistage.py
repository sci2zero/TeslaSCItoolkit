"""tesci.similarity.merge(): the multi-stage/two-source orchestration wrapper.

Unlike test_merge_synthetic.py (which drives ``_merge_two_sources`` directly
to test the matching *algorithm*), this exercises the public ``merge()``
entry point -- the part responsible for picking source pairs and stage
numbers and chaining stage outputs together.

The flat two-source path works again: ``merge()`` had been refactored in
commit a2cb830 to always pass ``stage=i+1`` rather than ``stage=None`` for a
flat single-stage config, and now consults ``_has_stage_config()`` instead.
The 3+ source chaining path is still broken (see the remaining ``xfail``);
a passing test at the bottom manually performs the chaining ``merge()`` is
supposed to do, to show the underlying algorithm still composes correctly --
that's the behavior a fix should reproduce.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import tesci.similarity as similarity
from tests.helpers import merge_column, multi_stage_config, similarity_config
from tests.helpers.synthetic import scopus_like_frame, synthetic_similarity_columns, wos_like_frame


def test_two_source_merge_with_flat_config_succeeds(tesci_project, captured_outputs):
    # Source order is not free: `_merge_two_sources` reads ``into_`` off the FIRST
    # source and ``from_`` off the SECOND (see
    # test_source_order_follows_into_then_from below). synthetic_similarity_columns()
    # maps from_="title" into_="article title", and in these fixtures "title" lives
    # on wos_like_frame, so wos_like_frame has to be the *second* source.
    # NB the real WoS/Scopus exports are the other way round -- WoS carries
    # "Article Title" and Scopus carries "Title" -- so a real run passes WoS first.
    tesci_project.write_config(similarity_config(synthetic_similarity_columns()))
    wos = tesci_project.add_csv("wos.csv", wos_like_frame())
    scopus = tesci_project.add_csv("scopus.csv", scopus_like_frame())

    result = tesci_project.run("similarity", "merge", "-s", str(scopus), "-s", str(wos))

    assert result.exit_code == 0, result.output


def test_source_order_follows_into_then_from(tesci_project, captured_outputs):
    """Pin the orientation contract: ``--src <into_ side> --src <from_ side>``.

    ``_merge_two_sources`` resolves ``reference_column["into_"]`` against df1 (the
    first source) and ``reference_column["from_"]`` against df2 (the second), so
    passing the sources the other way round raises ``KeyError`` on the reference
    column. This is worth pinning because the ``from_``/``into_`` naming reads as
    though ``from_`` should belong to the first source, and because the order
    silently determines which schema the merged output is written in.
    """
    tesci_project.write_config(similarity_config(synthetic_similarity_columns()))
    wos = tesci_project.add_csv("wos.csv", wos_like_frame())
    scopus = tesci_project.add_csv("scopus.csv", scopus_like_frame())

    correct = tesci_project.run("similarity", "merge", "-s", str(scopus), "-s", str(wos))
    assert correct.exit_code == 0, correct.output

    reversed_ = tesci_project.run("similarity", "merge", "-s", str(wos), "-s", str(scopus))
    assert reversed_.exit_code != 0
    assert isinstance(reversed_.exception, KeyError)


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="At stage i>0, merge() reassigns name_override for the *current* stage "
    "before computing second_src from it, so second_src points at the file this "
    "stage is about to write, not the previous stage's output -- FileNotFoundError "
    "for any chain of 3+ sources.",
)
def test_three_source_multistage_merge_chains_stage_outputs(tesci_project, captured_outputs):
    title_column = [merge_column("title", "title", above=90, cutoff=50, is_reference=True)]
    tesci_project.write_config(multi_stage_config([title_column, title_column]))
    a = tesci_project.add_csv("a.csv", [{"title": "Alpha Study"}, {"title": "Beta Study"}])
    b = tesci_project.add_csv("b.csv", [{"title": "Alpha Study"}, {"title": "Beta Study"}])
    c = tesci_project.add_csv("c.csv", [{"title": "Alpha Study"}, {"title": "Beta Study"}])

    result = tesci_project.run("similarity", "merge", "-s", str(a), "-s", str(b), "-s", str(c))

    assert result.exit_code == 0, result.output
    assert "config-final.xls" in captured_outputs


def test_manually_chaining_two_merge_stages_composes(tesci_project, captured_outputs):
    """What the multi-stage orchestration in merge() is supposed to do.

    Stage 1 merges WoS+Scopus synthetic rows (test_merge_synthetic.py's
    fixtures); stage 2 takes that stage's ``final_df`` output and merges it
    against a third, Scopus-shaped source using an identity column mapping.
    This shows the classification algorithm itself composes correctly across
    stages -- the bug above is in ``merge()``'s bookkeeping, not here.
    """
    tesci_project.write_config({})
    wos_path = tesci_project.add_csv("wos.csv", wos_like_frame())
    scopus_path = tesci_project.add_csv("scopus.csv", scopus_like_frame())
    stage1_config = SimpleNamespace(content=similarity_config(synthetic_similarity_columns()))

    similarity._merge_two_sources(
        scopus_path,
        wos_path,
        stage1_config,
        stage=None,
        save_to_disk_name_override="config-stage_1_merged.xls",
        dest=None,
    )
    stage1_output_path = tesci_project.path("config-stage_1_merged.xls")
    assert stage1_output_path.exists()

    third_source = tesci_project.add_csv(
        "third.csv", [{"title": "Study of Alpha Particles in Layered Semiconductors"}]
    )
    stage2_columns = [
        merge_column("article title", "title", above=90, cutoff=50, is_reference=True)
    ]
    stage2_config = SimpleNamespace(content=similarity_config(stage2_columns))

    similarity._merge_two_sources(
        third_source,
        stage1_output_path,
        stage2_config,
        stage=None,
        save_to_disk_name_override="config-final.xls",
        dest=None,
    )

    final_df = captured_outputs["config-final.xls"]
    assert "Study of Alpha Particles in Layered Semiconductors" in final_df["title"].values
