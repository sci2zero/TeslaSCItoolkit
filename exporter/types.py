import attrs


@attrs.define
class Aggregate:
    function: str = attrs.field(default="")
    columns: list[str] = attrs.field(default=[""])
    alias: str = attrs.field(default="")
