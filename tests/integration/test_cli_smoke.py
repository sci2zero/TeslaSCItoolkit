"""Cheap end-to-end checks that the CLI wiring itself isn't broken.

These don't assert much about business logic -- their job is to fail
loudly and immediately if a rewrite breaks command registration or an
import, which is otherwise the kind of thing that only surfaces when a
user runs the real binary.
"""

from __future__ import annotations

import pytest

TOP_LEVEL_COMMANDS = [
    "aggregate",
    "apply",
    "download",
    "similarity",
    "join",
    "include",
    "preview",
    "release",
    "start",
]

SIMILARITY_SUBCOMMANDS = ["merge", "suggest"]


def test_cli_help_exits_cleanly(tesci_project):
    result = tesci_project.run("--help")

    assert result.exit_code == 0
    assert "Usage" in result.output


@pytest.mark.parametrize("command", TOP_LEVEL_COMMANDS)
def test_each_top_level_command_registers_and_has_help(tesci_project, command):
    result = tesci_project.run(command, "--help")

    assert result.exit_code == 0, result.output


@pytest.mark.parametrize("subcommand", SIMILARITY_SUBCOMMANDS)
def test_similarity_subcommands_register(tesci_project, subcommand):
    result = tesci_project.run("similarity", subcommand, "--help")

    assert result.exit_code == 0, result.output


def test_apply_aborts_without_confirmation(tesci_project):
    tesci_project.add_csv("in.csv", [{"a": 1}])
    tesci_project.write_config({"data": {"src": "in.csv", "dest": "out.csv"}})

    result = tesci_project.run("apply", input="n\n")

    assert result.exit_code == 0
    assert "Aborted" in result.output
    assert not tesci_project.exists("out.csv")


def test_apply_runs_without_prompt_under_ci_env(tesci_project, ci_env):
    tesci_project.add_csv("in.csv", [{"a": 1}])
    tesci_project.write_config({"data": {"src": "in.csv", "dest": "out.csv"}})

    result = tesci_project.run("apply")

    assert result.exit_code == 0, result.output
    assert tesci_project.exists("out.csv")


@pytest.mark.known_bug
@pytest.mark.xfail(
    strict=True,
    reason="Config() (called from Config.init, called from `tesci start`) requires "
    "config.yml to already exist, so `start` on a brand-new project can't create one.",
)
def test_start_initializes_a_brand_new_project(tesci_project):
    result = tesci_project.run("start", "-d", "in.csv", "-o", "out.csv")

    assert result.exit_code == 0, result.output
    assert tesci_project.read_config()["data"] == {"src": "in.csv", "dest": "out.csv"}
