"""Reusable test harness for tesci.

The pieces here exist to absorb the parts of tesci that are awkward to test
directly, so that individual tests stay short and survive refactors:

* :mod:`~tests.helpers.project` hides the fact that ``Config``/``DataSource``
  resolve everything relative to ``Path.cwd()``.
* :mod:`~tests.helpers.configs` builds config dictionaries, always fresh,
  because the merge code mutates the config it is handed.
* :mod:`~tests.helpers.frames` compares dataframes without depending on
  column order.
* :mod:`~tests.helpers.synthetic` generates citation records with a known,
  self-verifying expected outcome per row.
"""

from tests.helpers.configs import (
    aggregate_entry,
    merge_column,
    multi_stage_config,
    similarity_config,
)
from tests.helpers.frames import (
    assert_frame_equals_unordered,
    assert_same_rows,
    column_set,
)
from tests.helpers.project import TesciProject
from tests.helpers.synthetic import (
    Bucket,
    SyntheticCase,
    scopus_like_frame,
    synthetic_cases,
    wos_like_frame,
)

__all__ = [
    "Bucket",
    "SyntheticCase",
    "TesciProject",
    "aggregate_entry",
    "assert_frame_equals_unordered",
    "assert_same_rows",
    "column_set",
    "merge_column",
    "multi_stage_config",
    "scopus_like_frame",
    "similarity_config",
    "synthetic_cases",
    "wos_like_frame",
]
