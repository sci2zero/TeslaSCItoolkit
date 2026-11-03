"""tesci.adjudicate: prompt rendering, response schemas and the decision cache.

Nothing here contacts a model; every piece under test is pure.
"""

from __future__ import annotations

import json
import threading

import pytest

from tesci.adjudicate import (
    DecisionCache,
    OllamaRuntime,
    format_pair,
    format_pair_terse,
    normalise_decision,
    response_schema,
)

FIELDS = ["title", "year", "doi"]

RECORD = {
    "wos_title": "Smart Multi-Agent Framework for Automated Audio Testing",
    "scopus_title": "Smart multi-agent framework for automated audio testing",
    "wos_year": 2024,
    "scopus_year": 2024,
    "wos_doi": "10.5755/J02.EIE.33222",
    "scopus_doi": None,
}


class TestResponseSchema:
    def test_verbose_schema_constrains_decided_by_to_the_given_fields(self):
        schema = response_schema(FIELDS)
        assert schema["properties"]["decision"]["enum"] == [
            "duplicate",
            "not_duplicate",
            "abstain",
        ]
        assert schema["properties"]["decided_by"]["items"]["enum"] == FIELDS
        assert set(schema["required"]) == {"decision", "confidence", "decided_by"}

    def test_terse_schema_asks_for_one_letter(self):
        schema = response_schema(FIELDS, terse=True)
        assert schema["properties"] == {"d": {"type": "string", "enum": ["Y", "N"]}}
        assert schema["required"] == ["d"]


class TestNormaliseDecision:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [({"d": "Y"}, "duplicate"), ({"d": "N"}, "not_duplicate")],
    )
    def test_terse_reply_maps_onto_the_verbose_vocabulary(self, raw, expected):
        assert normalise_decision(raw)["decision"] == expected

    def test_terse_reply_carries_no_confidence(self):
        assert normalise_decision({"d": "Y"})["confidence"] is None

    def test_verbose_reply_passes_through(self):
        out = normalise_decision(
            {"decision": "duplicate", "confidence": 0.95, "decided_by": ["doi"]}
        )
        assert out == {"decision": "duplicate", "confidence": 0.95, "decided_by": ["doi"]}

    def test_missing_decided_by_becomes_a_list(self):
        assert normalise_decision({"decision": "abstain", "confidence": 0.1})["decided_by"] == []


class TestPromptRendering:
    def test_verbose_prompt_labels_every_field_on_both_sides(self):
        prompt = format_pair(RECORD, FIELDS, "wos_", "scopus_")
        for name in FIELDS:
            assert f"  {name}:" in prompt
        assert prompt.count("    A: ") == len(FIELDS)
        assert prompt.count("    B: ") == len(FIELDS)

    def test_terse_prompt_is_one_line_per_field(self):
        prompt = format_pair_terse(RECORD, FIELDS, "wos_", "scopus_")
        assert len(prompt.splitlines()) == len(FIELDS)
        assert " :: " in prompt

    def test_terse_prompt_is_shorter_than_verbose(self):
        verbose = format_pair(RECORD, FIELDS, "wos_", "scopus_")
        terse = format_pair_terse(RECORD, FIELDS, "wos_", "scopus_")
        assert len(terse) < len(verbose)

    @pytest.mark.parametrize("empty", [None, "", "   ", "nan", "NaN", "none"])
    def test_an_absent_value_is_rendered_as_missing_not_as_nan(self, empty):
        prompt = format_pair({"a_x": "present", "b_x": empty}, ["x"], "a_", "b_")
        assert "(missing)" in prompt
        assert "nan" not in prompt.lower().replace("(missing)", "")

    def test_a_long_value_is_clipped(self):
        long = "x" * 900
        prompt = format_pair({"a_x": long, "b_x": "short"}, ["x"], "a_", "b_")
        assert len(prompt) < len(long)
        assert "…" in prompt


class TestDecisionCache:
    def test_key_is_stable_for_the_same_inputs(self):
        identity = {"model": "m", "digest": "d"}
        schema = response_schema(FIELDS)
        assert DecisionCache.key(identity, schema, "p") == DecisionCache.key(
            identity, schema, "p"
        )

    def test_key_changes_with_the_model_digest(self):
        # A model re-pulled under the same tag must not replay the old answers.
        schema = response_schema(FIELDS)
        a = DecisionCache.key({"model": "m", "digest": "one"}, schema, "p")
        b = DecisionCache.key({"model": "m", "digest": "two"}, schema, "p")
        assert a != b

    def test_key_changes_with_the_schema(self):
        identity = {"model": "m", "digest": "d"}
        verbose = DecisionCache.key(identity, response_schema(FIELDS), "p")
        terse = DecisionCache.key(identity, response_schema(FIELDS, terse=True), "p")
        assert verbose != terse

    def test_key_changes_with_the_prompt(self):
        identity = {"model": "m", "digest": "d"}
        schema = response_schema(FIELDS)
        assert DecisionCache.key(identity, schema, "a") != DecisionCache.key(
            identity, schema, "b"
        )

    def test_a_decision_survives_a_reload(self, tmp_path):
        path = tmp_path / "cache.jsonl"
        decision = {"decision": "duplicate", "confidence": 0.9, "decided_by": ["doi"]}
        DecisionCache(path).put("k", decision, {"seconds": 1.0})

        reloaded = DecisionCache(path)
        assert len(reloaded) == 1
        assert reloaded.get("k")["decision"] == decision

    def test_an_unknown_key_returns_none(self, tmp_path):
        assert DecisionCache(tmp_path / "cache.jsonl").get("absent") is None

    def test_a_malformed_line_is_skipped_rather_than_fatal(self, tmp_path):
        path = tmp_path / "cache.jsonl"
        good = {"key": "k", "decision": {"decision": "duplicate"}, "meta": {}}
        path.write_text("not json\n" + json.dumps(good) + "\n", encoding="utf-8")
        cache = DecisionCache(path)
        assert len(cache) == 1
        assert cache.get("k") is not None

    def test_concurrent_writes_all_land(self, tmp_path):
        # Interleaved appends would leave the file unparseable.
        cache = DecisionCache(tmp_path / "cache.jsonl")
        threads = [
            threading.Thread(target=cache.put, args=(f"k{i}", {"decision": "x"}, {}))
            for i in range(24)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(DecisionCache(tmp_path / "cache.jsonl")) == 24


class TestRuntimeDefaults:
    def test_decisions_are_deterministic_by_default(self):
        runtime = OllamaRuntime()
        assert runtime.temperature == 0.0
        assert runtime.seed is not None
        assert runtime.think is False

    def test_identity_degrades_to_an_empty_digest_when_the_runtime_is_unreachable(self):
        runtime = OllamaRuntime(base_url="http://localhost:1")
        identity = runtime.identity()
        assert identity["model"] == runtime.model
        assert identity["digest"] == ""
