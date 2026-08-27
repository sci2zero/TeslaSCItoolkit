"""Guard against the config schema silently drifting away from documentation.

tesci has no schema/validation layer (raw ``yaml.safe_load`` into dicts,
see ``tesci.scripts.context.Config``), so ``examples/*.yml`` and the
tracked-but-real ``.tesci/config.yml`` are the closest thing to a spec.
This test just parses every one of them and checks their top-level keys
are all ones the code actually knows about -- cheap, and it catches a
config example (or a config-reading rewrite) drifting out of sync with
the other.
"""

from __future__ import annotations

import yaml
import pytest

KNOWN_TOP_LEVEL_KEYS = {
    "description",
    "data",
    "include",
    "aggregate",
    "sort",
    "join",
    "download",
    "release",
}


def _all_example_configs(repo_root):
    paths = sorted((repo_root / "examples").rglob("*.yml"))
    tesci_config = repo_root / ".tesci" / "config.yml"
    if tesci_config.exists():
        paths.append(tesci_config)
    return paths


def test_at_least_one_example_config_is_found(repo_root):
    assert _all_example_configs(repo_root), "no example configs found under examples/"


def pytest_generate_tests(metafunc):
    if "config_path" in metafunc.fixturenames:
        from tests.conftest import REPO_ROOT

        paths = _all_example_configs(REPO_ROOT)
        metafunc.parametrize("config_path", paths, ids=[p.stem for p in paths])


def test_example_config_uses_only_known_top_level_keys(config_path):
    with open(config_path) as fh:
        content = yaml.safe_load(fh)

    assert isinstance(content, dict), f"{config_path} did not parse to a mapping"
    unknown = set(content.keys()) - KNOWN_TOP_LEVEL_KEYS
    assert not unknown, f"{config_path.name} has unrecognized top-level keys: {unknown}"


@pytest.mark.known_bug
def test_examples_06_multi_source_shape_is_flagged_as_unimplemented(repo_root):
    """``examples/06-config-multiple-similarity-joins.yml`` documents an n-way
    ``src_1/src_2/src_3`` merge shape that ``similarity._merge_two_sources``
    does not implement (it only ever reads ``from_``/``into_``). This test
    doesn't touch the merge code -- it just pins down that the example is
    known-aspirational, so if a rewrite starts consuming it, this test (not
    a confusing runtime KeyError somewhere else) is the place to update.

    The file is a work-in-progress draft that may not always be committed;
    this test is a no-op when it's absent.
    """
    path = repo_root / "examples" / "06-config-multiple-similarity-joins.yml"
    if not path.exists():
        pytest.skip(f"{path.name} is not present in this checkout")

    with open(path) as fh:
        content = yaml.safe_load(fh)

    columns = content["join"]["similarity_config"]["merge"]["columns"]
    assert "src_1" in columns[0] and "from_" not in columns[0]
