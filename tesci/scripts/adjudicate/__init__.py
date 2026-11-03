from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import pandas as pd

from tesci.adjudicate import (
    DecisionCache,
    OllamaRuntime,
    format_pair,
    format_pair_terse,
    response_schema,
)

DEFAULT_FIELDS = "title,authors,year,doi,source_title,doc_type"


@click.option("-p", "--pairs", required=True, type=click.Path(exists=True, path_type=Path),
              help="CSV of candidate pairs to decide")
@click.option("-o", "--out", required=True, type=click.Path(path_type=Path),
              help="Where to write the decisions")
@click.option("--fields", default=DEFAULT_FIELDS, show_default=True,
              help="Comma-separated logical field names to show the judge")
@click.option("--left-prefix", default="wos_", show_default=True)
@click.option("--right-prefix", default="scopus_", show_default=True)
@click.option("--model", default="qwen3.5:4b", show_default=True)
@click.option("--base-url", default="http://localhost:11434", show_default=True,
              help="Local runtime endpoint; a non-local host is refused")
@click.option("--seed", default=42, show_default=True)
@click.option("--cache", default=None, type=click.Path(path_type=Path),
              help="Decision cache JSONL [default: alongside --out]")
@click.option("--limit", default=0, help="Only decide the first N pairs (0 = all)")
@click.option("--terse", is_flag=True, default=False,
              help="One line per field and a single-letter answer. Far fewer output tokens, "
                   "which dominate latency on CPU, at the cost of the confidence and "
                   "deciding-fields detail.")
@click.option("--workers", default=1, show_default=True,
              help="Concurrent requests to the runtime. Raises throughput on a multi-core "
                   "host; per-request latency is unchanged.")
@click.option("--allow-remote", is_flag=True, default=False,
              help="Permit a non-local base URL. Licensed records must not leave the machine.")
@click.command(name="adjudicate")
def adjudicate_cli(pairs, out, fields, left_prefix, right_prefix, model, base_url, seed,
                   cache, limit, terse, workers, allow_remote):
    """Have a local model decide candidate pairs the matcher left uncertain."""
    host = base_url.split("//", 1)[-1].split("/")[0].split(":")[0]
    if host not in ("localhost", "127.0.0.1", "::1", "0.0.0.0") and not allow_remote:
        raise click.UsageError(
            f"'{base_url}' is not a local runtime. Web of Science and Scopus licences do not "
            "permit processing licensed content through hosted AI services. Pass --allow-remote "
            "only for data you are certain may leave this machine."
        )

    field_names = [f.strip() for f in fields.split(",") if f.strip()]
    schema = response_schema(field_names, terse=terse)
    render = format_pair_terse if terse else format_pair
    runtime = OllamaRuntime(
        model=model, base_url=base_url, seed=seed,
        terse=terse, num_predict=24 if terse else 96,
    )
    identity = runtime.identity()
    if not identity.get("digest"):
        click.echo(f"warning: could not read a digest for '{model}'; "
                   "cache keys will not detect the model changing underneath you", err=True)

    df = pd.read_csv(pairs)
    if limit:
        df = df.head(limit)
    store = DecisionCache(cache or out.with_suffix(".cache.jsonl"))
    click.echo(f"{len(df)} pairs · model {identity['model']} "
               f"(digest {identity.get('digest','?')[:12]}) · cache holds {len(store)}")

    shown = {f"{left_prefix}{n}" for n in field_names} | {
        f"{right_prefix}{n}" for n in field_names
    }

    def decide_one(record) -> dict:
        prompt = render(record.to_dict(), field_names, left_prefix, right_prefix)
        key = DecisionCache.key(identity, schema, prompt)
        entry = store.get(key)
        if entry is not None:
            decision, meta = entry["decision"], entry["meta"]
        else:
            try:
                decision, meta = runtime.decide(prompt, schema)
            except Exception as exc:  # noqa: BLE001 - keep going, record the failure
                decision = {"decision": "error", "confidence": None, "decided_by": []}
                meta = {"error": f"{type(exc).__name__}: {exc}"}
            else:
                store.put(key, decision, meta)
        return {
            **{k: record.get(k) for k in record.index if k not in shown},
            "model": identity["model"],
            "model_digest": identity.get("digest"),
            "decision": decision.get("decision"),
            "confidence": decision.get("confidence"),
            "decided_by": ",".join(decision.get("decided_by") or []),
            "cache_hit": entry is not None,
            **{f"meta_{k}": v for k, v in meta.items()},
        }

    records = [record for _, record in df.iterrows()]
    with click.progressbar(length=len(records), label="adjudicating", show_pos=True) as bar:
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                rows = []
                for row in pool.map(decide_one, records):
                    rows.append(row)
                    bar.update(1)
        else:
            rows = []
            for record in records:
                rows.append(decide_one(record))
                bar.update(1)

    result = pd.DataFrame(rows)
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out, index=False)

    hits = int(result["cache_hit"].sum()) if len(result) else 0
    failures = int((result["decision"] == "error").sum()) if len(result) else 0
    click.echo(f"\ncache hits {hits}/{len(result)}   failures {failures}")
    click.echo(result["decision"].value_counts().to_string())
    if "meta_seconds" in result and result["meta_seconds"].notna().any():
        secs = pd.to_numeric(result["meta_seconds"], errors="coerce")
        click.echo(f"wall-clock: total {secs.sum():.0f}s  mean {secs.mean():.1f}s/pair")
    click.echo(f"wrote {out}")
