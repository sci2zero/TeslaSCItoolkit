"""Ask a locally-hosted model to decide candidate pairs the matcher left uncertain.

Scope is deliberately narrow. This does not replace the similarity matcher: it is
invoked on the band of pairs the matcher declined to merge, which is where a human
reviewer's time goes. Everything runs against a local runtime (Ollama by default);
no record ever leaves the machine, because Web of Science and Scopus licences do
not permit processing licensed content through hosted AI services.

Reproducibility is the point of the cache. Every decision is keyed by a hash of
the model identity (name *and* digest), the response schema, the prompt template
version and the normalised prompt, then appended to a JSONL file. A second run
with a warm cache re-derives byte-identical output and costs nothing, so a result
in a paper can be re-checked years later without re-running inference.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

PROMPT_VERSION = "pairwise-v1"

SYSTEM_PROMPT = (
    "You decide whether two bibliographic records describe the SAME publication. "
    "Record A comes from one citation database, record B from another. Field values "
    "may differ in formatting, capitalisation, abbreviation, language or completeness "
    "without meaning the records describe different works; a title may be truncated, "
    "an author list may be shortened, a DOI may differ only in case, and one side may "
    "leave a field empty. Answer 'duplicate' only if they are the same publication, "
    "'not_duplicate' if they are different works, and 'abstain' only if the fields "
    "given genuinely do not permit a decision. List the fields that decided it."
)


TERSE_SYSTEM_PROMPT = (
    "Each line is 'A :: B' — one field of two bibliographic records, in the order given. "
    "Values may differ in formatting, abbreviation, language or completeness without meaning "
    "the records are different works. Are they the same publication? Y or N."
)


def response_schema(fields: list[str], terse: bool = False) -> dict:
    """Schema for the judge's reply.

    The verbose form asks for a confidence and the deciding fields. Measured on the
    FTN band, the confidence is useless -- effectively constant at 0.95 whether the
    answer is right or wrong -- while those two extra properties cost ~85% of the
    output tokens, and output tokens dominate latency on CPU. `terse` drops them and
    asks for a single letter, which is ~7 output tokens instead of ~51.
    """
    if terse:
        return {
            "type": "object",
            "properties": {"d": {"type": "string", "enum": ["Y", "N"]}},
            "required": ["d"],
        }
    return {
        "type": "object",
        "properties": {
            "decision": {"type": "string", "enum": ["duplicate", "not_duplicate", "abstain"]},
            "confidence": {"type": "number"},
            "decided_by": {"type": "array", "items": {"type": "string", "enum": list(fields)}},
        },
        "required": ["decision", "confidence", "decided_by"],
    }


def normalise_decision(raw: dict) -> dict:
    """Map either schema's reply onto one shape."""
    if "d" in raw:
        return {
            "decision": "duplicate" if raw["d"] == "Y" else "not_duplicate",
            "confidence": None,
            "decided_by": [],
        }
    return {
        "decision": raw.get("decision"),
        "confidence": raw.get("confidence"),
        "decided_by": raw.get("decided_by") or [],
    }


@dataclass
class OllamaRuntime:
    """Local Ollama. `think=False` keeps short-form models from emitting reasoning."""

    model: str = "qwen3.5:4b"
    base_url: str = "http://localhost:11434"
    seed: int = 42
    temperature: float = 0.0
    num_predict: int = 96
    num_ctx: int = 0
    keep_alive: str = "10m"
    think: bool = False
    terse: bool = False
    timeout: int = 600
    _digest: str | None = field(default=None, init=False, repr=False)

    def identity(self) -> dict:
        """Model name plus digest, so a silently re-pulled model invalidates the cache."""
        if self._digest is None:
            self._digest = ""
            try:
                with urllib.request.urlopen(f"{self.base_url}/api/tags", timeout=15) as r:
                    for m in json.loads(r.read()).get("models", []):
                        if m.get("name") == self.model:
                            self._digest = m.get("digest", "")
                            break
            except (urllib.error.URLError, OSError, ValueError):
                pass
        return {"runtime": "ollama", "model": self.model, "digest": self._digest}

    def decide(self, prompt: str, schema: dict) -> tuple[dict, dict]:
        body = {
            "model": self.model,
            "stream": False,
            "think": self.think,
            "format": schema,
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": self.temperature,
                "seed": self.seed,
                "num_predict": self.num_predict,
                **({"num_ctx": self.num_ctx} if self.num_ctx else {}),
            },
            "messages": [
                {"role": "system", "content": TERSE_SYSTEM_PROMPT if self.terse else SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }
        req = urllib.request.Request(
            f"{self.base_url}/api/chat",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        started = time.time()
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            raw = json.loads(r.read())
        elapsed = time.time() - started
        parsed = normalise_decision(json.loads(raw["message"]["content"]))
        meta = {
            "seconds": round(elapsed, 2),
            "prompt_tokens": raw.get("prompt_eval_count"),
            "output_tokens": raw.get("eval_count"),
        }
        return parsed, meta


def format_pair(record: dict, fields: list[str], left_prefix: str, right_prefix: str) -> str:
    lines = ["Record pair:"]
    for name in fields:
        a = record.get(f"{left_prefix}{name}")
        b = record.get(f"{right_prefix}{name}")
        lines.append(f"  {name}:")
        lines.append(f"    A: {_clip(a)}")
        lines.append(f"    B: {_clip(b)}")
    return "\n".join(lines)


def format_pair_terse(record: dict, fields: list[str], left_prefix: str, right_prefix: str,
                      limit: int = 60) -> str:
    return "\n".join(
        f"{_clip(record.get(f'{left_prefix}{n}'), limit)} :: "
        f"{_clip(record.get(f'{right_prefix}{n}'), limit)}"
        for n in fields
    )


def _clip(value, limit: int = 320) -> str:
    text = "" if value is None else str(value)
    if text.strip().lower() in ("", "nan", "none"):
        return "(missing)"
    return text if len(text) <= limit else text[: limit - 1] + "…"


class DecisionCache:
    """Append-only JSONL cache. Commit it alongside results to make a run replayable."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._entries: dict[str, dict] = {}
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if "key" in entry:
                    self._entries[entry["key"]] = entry

    @staticmethod
    def key(identity: dict, schema: dict, prompt: str) -> str:
        payload = json.dumps(
            {
                "identity": identity,
                "schema": schema,
                "prompt_version": PROMPT_VERSION,
                "prompt": prompt,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def get(self, key: str) -> dict | None:
        return self._entries.get(key)

    def put(self, key: str, decision: dict, meta: dict) -> None:
        entry = {"key": key, "decision": decision, "meta": meta}
        self._entries[key] = entry
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, sort_keys=True) + "\n")

    def __len__(self) -> int:
        return len(self._entries)
