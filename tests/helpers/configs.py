"""Config-dict builders.

``tesci`` has no schema layer (raw ``yaml.safe_load`` into dicts, see
``tesci.scripts.context.Config``), and ``_merge_two_sources`` mutates the
``columns`` list it's handed in place (lower-cases ``from_``/``into_``).
These builders return a brand-new dict on every call so a test can build a
config, hand it to the code under test, and not worry about leaking state
into the next test.
"""

from __future__ import annotations

from typing import Any


def merge_column(
    from_: str,
    into_: str,
    *,
    above: float,
    cutoff: float,
    is_reference: bool = False,
    preprocess: dict[str, Any] | None = None,
    cast_to_str: bool = False,
) -> dict[str, Any]:
    """One ``similarity_config.merge[.stage_N].columns[]`` entry.

    ``cast_to_str`` sets ``similarity.preprocess: yes`` without an actual
    ``preprocess:`` block, matching the config shape needed to run
    non-string columns (e.g. a year) through ``safe_cast_to_str`` without
    truncating or replacing anything.
    """
    column: dict[str, Any] = {
        "from_": from_,
        "into_": into_,
        "similarity": {
            "above": above,
            "cutoff": cutoff,
            "preprocess": preprocess is not None or cast_to_str,
        },
    }
    if is_reference:
        column["is_reference"] = True
    if preprocess is not None:
        column["preprocess"] = preprocess
    return column


def similarity_config(
    columns: list[dict[str, Any]],
    *,
    drop_duplicates: bool = False,
) -> dict[str, Any]:
    """A single-stage ``join:`` block, as in ``examples/04-config-similarity-join.yml``."""
    merge: dict[str, Any] = {"columns": columns}
    if drop_duplicates:
        merge["drop_duplicates"] = True
    return {
        "join": {
            "similarity": True,
            "similarity_config": {"merge": merge},
        }
    }


def multi_stage_config(
    stages: list[list[dict[str, Any]]],
    *,
    drop_duplicates: bool = False,
) -> dict[str, Any]:
    """A ``multi_stage: yes`` ``join:`` block with one ``stage_N`` per entry in ``stages``."""
    merge: dict[str, Any] = {}
    if drop_duplicates:
        merge["drop_duplicates"] = True
    for i, columns in enumerate(stages, start=1):
        merge[f"stage_{i}"] = {"columns": columns}
    return {
        "join": {
            "similarity": True,
            "multi_stage": True,
            "similarity_config": {"merge": merge},
        }
    }


def aggregate_entry(
    alias: str,
    columns: list[str],
    function: str,
    grouped: str | list[str] | None = None,
) -> dict[str, Any]:
    """One ``aggregate:`` list entry."""
    entry: dict[str, Any] = {"alias": alias, "columns": list(columns), "function": function}
    if grouped is not None:
        entry["grouped"] = grouped
    return entry
