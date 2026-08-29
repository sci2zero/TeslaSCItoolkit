"""tesci.scripts.context: Config, Reader, DataSource.

These are pure dict/YAML/file-path mechanics with no similarity or
aggregation logic involved, so they're tested directly against
``tesci.scripts.context`` rather than through the CLI.
"""

from __future__ import annotations

import textwrap

import pytest
import yaml

from tesci.scripts.context import Config, DataSource, Reader


def test_loads_yaml_content(tesci_project):
    tesci_project.write_config({"data": {"src": "in.csv", "dest": "out.csv"}})

    config = Config()

    assert config.content == {"data": {"src": "in.csv", "dest": "out.csv"}}


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="__init__ sets _content = _read_config() or {}, but the `content` property "
    "re-reads from disk whenever _content is falsy -- including a legitimately empty "
    "{} -- discarding that fallback and returning None instead.",
)
def test_empty_config_file_yields_empty_dict(tesci_project):
    tesci_project.path("config.yml").write_text("")

    assert Config().content == {}


def test_malformed_yaml_raises_yaml_error(tesci_project):
    tesci_project.path("config.yml").write_text(
        textwrap.dedent(
            """\
            data:
              src: in.csv
            \tdest: out.csv
            """
        )
    )

    with pytest.raises(yaml.YAMLError):
        Config()


@pytest.mark.known_bug
@pytest.mark.xfail(strict=True, reason="_read_config references self.yaml_file_path, which doesn't exist")
def test_missing_config_file_raises_file_not_found(tesci_project):
    with pytest.raises(FileNotFoundError):
        Config()


def test_content_write_round_trips(tesci_project):
    tesci_project.write_config({"data": {"src": "in.csv", "dest": "out.csv"}})
    config = Config()

    config.content["include"] = ["a", "b"]
    config.write()

    assert Config().content == {
        "data": {"src": "in.csv", "dest": "out.csv"},
        "include": ["a", "b"],
    }


def test_dest_name_and_destination_path(tesci_project):
    tesci_project.write_config({"data": {"src": "in.csv", "dest": "out.csv"}})
    config = Config()

    assert config.dest_name == "out.csv"
    assert config.destination_path == tesci_project.path("out.csv")


def test_init_sets_data_source_and_dest(tesci_project):
    tesci_project.write_config({"data": {"src": "old.csv", "dest": "old_out.csv"}})

    Config.init("new.csv", "new_out.csv")

    assert Config().content["data"] == {"src": "new.csv", "dest": "new_out.csv"}


def test_reader_loads_csv(tesci_project):
    path = tesci_project.add_csv("in.csv", [{"a": 1, "b": 2}])

    df = Reader(path).df

    assert df.to_dict(orient="records") == [{"a": 1, "b": 2}]


def test_reader_loads_xlsx(tesci_project):
    path = tesci_project.add_excel("in.xlsx", [{"a": 1, "b": 2}])

    df = Reader(path).df

    assert df.to_dict(orient="records") == [{"a": 1, "b": 2}]


def test_reader_unsupported_extension_raises(tesci_project):
    path = tesci_project.path("in.txt")
    path.write_text("a,b\n1,2\n")

    with pytest.raises(ValueError, match="not supported"):
        Reader(path)


def test_reader_missing_file_raises(tesci_project):
    with pytest.raises(FileNotFoundError):
        Reader(tesci_project.path("missing.csv"))


def test_reader_resolves_relative_path_under_tesci_dir(tesci_project):
    tesci_project.add_csv("in.csv", [{"a": 1}])

    df = Reader("in.csv").df

    assert list(df.columns) == ["a"]


def test_data_source_requires_data_or_join(tesci_project):
    tesci_project.write_config({"data": {}})

    with pytest.raises(ValueError, match="No data source"):
        DataSource()


def test_data_source_rejects_both_data_and_join(tesci_project):
    tesci_project.add_csv("in.csv", [{"a": 1}])
    tesci_project.add_csv("j.csv", [{"a": 1}])
    tesci_project.write_config(
        {
            "data": {"src": "in.csv", "dest": "out.csv"},
            "join": {"src": ["j.csv"], "columns": ["a"]},
        }
    )

    with pytest.raises(ValueError, match="cannot specify both"):
        DataSource()


def test_data_source_loads_join_sources_with_defaults(tesci_project):
    tesci_project.add_csv("a.csv", [{"k": 1, "v": "x"}])
    tesci_project.add_csv("b.csv", [{"k": 1, "v": "y"}])
    tesci_project.write_config(
        {
            "data": {"dest": "out.csv"},
            "join": {"src": ["a.csv", "b.csv"], "columns": ["k"]},
        }
    )

    data = DataSource()

    assert data.join_sources is not None
    assert data.join_sources.how == "left"
    assert data.join_sources.fuzzy is False
    assert len(data.join_sources.sources) == 2


@pytest.mark.known_bug
@pytest.mark.xfail(strict=True, reason="`.get('data', None).get(...)` AttributeErrors when 'data' key is absent")
def test_data_source_load_without_data_or_join_key_raises_value_error(tesci_project):
    tesci_project.write_config({"description": "no data or join key here"})

    with pytest.raises(ValueError):
        DataSource()


def test_data_source_saves_xls_on_current_pandas_stack(tesci_project):
    """Regression guard for a genuinely surprising pandas quirk.

    pandas 2.x no longer registers an ``io.excel.xls.writer`` option, and
    ``df.to_excel("name.xls")`` (a plain ``str`` path) raises
    ``ValueError: No engine for filetype: 'xls'`` as a result. But
    ``DataSource.save_to_file`` always builds its destination as a
    ``pathlib.Path`` (see ``get_file_path``), and ``df.to_excel(Path(...))``
    resolves an engine (openpyxl) just fine for the exact same extension.
    Whether that Path-vs-str distinction survives a pandas upgrade is
    exactly what this test is here to catch.
    """
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2]})
    tesci_project.write_config({"data": {"src": "in.csv", "dest": "out.xls"}})

    DataSource.save_to_file(df, Config())

    assert tesci_project.exists("out.xls")
    assert pd.read_excel(tesci_project.path("out.xls"))["a"].tolist() == [1, 2]
