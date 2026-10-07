import json
from pathlib import Path

import click

import tesci.evaluation as evaluation


@click.option("-g", "--gold", required=True, type=click.Path(exists=True, path_type=Path),
              help="CSV of gold pairs, one row per true duplicate pair")
@click.option("-p", "--predicted", required=True, type=click.Path(exists=True, path_type=Path),
              help="The *-pairs.csv written by `tesci similarity merge`")
@click.option("--left-col", default="scopus_eid", show_default=True,
              help="Gold column holding the first source's record id")
@click.option("--right-col", default="wos_ut", show_default=True,
              help="Gold column holding the second source's record id")
@click.option("--all-buckets", is_flag=True, default=False,
              help="Score every decision, not just the exact/suggested merges")
@click.option("--by-stratum", "strata", multiple=True,
              help="Gold column to break the scores down by; repeatable")
@click.option("-o", "--out", type=click.Path(path_type=Path), default=None,
              help="Also write the report as JSON")
@click.command(name="eval")
def eval_cli(gold, predicted, left_col, right_col, all_buckets, strata, out):
    """Score a similarity merge against a gold standard, pair by pair."""
    report = evaluation.evaluate(
        gold_path=gold,
        predicted_path=predicted,
        left_col=left_col,
        right_col=right_col,
        merged_only=not all_buckets,
        strata=tuple(strata),
    )
    click.echo(evaluation.format_report(report))
    if out is not None:
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        click.echo(f"\nwrote {out}")
