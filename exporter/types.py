import attrs


@attrs.define
class Aggregate:
    function: str = attrs.field(default="")
    column: str = attrs.field(default="")
    alias: str = attrs.field(default="")
