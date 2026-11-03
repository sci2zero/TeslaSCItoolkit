"""tesci.evaluation against small on-disk fixtures."""

from __future__ import annotations

import pandas as pd

from tesci.evaluation import legacy_title_set_scores


def test_legacy_title_scores_ignore_blank_titles(tmp_path):
    manual = tmp_path / "manual.csv"
    automatic = tmp_path / "automatic.csv"
    pd.DataFrame({"Title": ["Alpha", "Beta", None]}).to_csv(manual, index=False)
    pd.DataFrame({"Title": ["Alpha", "Gamma", None]}).to_csv(automatic, index=False)

    scores = legacy_title_set_scores(manual, automatic)

    assert (scores["tp"], scores["fp"], scores["fn"]) == (1, 1, 1)
    assert (scores["manual_titles"], scores["automatic_titles"]) == (2, 2)
