import attrs


@attrs.define
class Aggregate:
    function: str = attrs.field(default="")
    columns: list[str] = attrs.field(default=[""])
    alias: str = attrs.field(default="")


@attrs.define
class FuzzyColumnCandidates:
    column: str = attrs.field(default="")
    reference_data: str = attrs.field(default="")
    fuzzy_matches: list[tuple[str, str, str]] = attrs.field(default=[])


@attrs.define
class MatchesPerColumn:
    column_candidates: dict[str, FuzzyColumnCandidates] = attrs.field(default={})


# Author:
# ORCID -
# Authors -
# .. -
