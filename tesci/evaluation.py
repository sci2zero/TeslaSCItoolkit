"""Pair-level evaluation of a similarity merge against a gold standard.

The 2024 study scored a merge by comparing *sets of normalised titles* between
the automatic and the manual output. That measures how much the two agree on
which titles survive, but it cannot tell a correct merge from a merge of the
wrong two records that happen to share a title, and it says nothing about which
Scopus record was matched to which WoS record. This module scores *pairs*:

    TP  pairs the matcher merged that the gold standard also pairs
    FP  pairs the matcher merged that the gold standard does not pair
    FN  gold pairs the matcher failed to merge

Pairs are compared as unordered id sets, so the source order a merge was run in
cannot silently invert the result.

``legacy_title_set_scores`` reproduces the 2024 measure as well, so a run can be
reported under both definitions and any divergence between them is visible
rather than hidden.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from rapidfuzz import utils

MERGED_BUCKETS = ("exact", "suggested")


def calculate_results(true_positive: int, false_positive: int, false_negative: int):
    """Precision, recall and F-measure.

    Kept identical to the 2024 study's ``compare.py`` so the numbers remain
    comparable with the published tables.
    """
    if true_positive + false_positive > 0:
        precision = true_positive / (true_positive + false_positive)
    else:
        precision = 0
    if true_positive + false_negative > 0:
        recall = true_positive / (true_positive + false_negative)
    else:
        recall = 0
    if precision + recall > 0:
        f1 = 2 * precision * recall / (precision + recall)
    else:
        f1 = 0
    return precision, recall, f1


def _scores(tp: int, fp: int, fn: int) -> dict:
    precision, recall, f1 = calculate_results(tp, fp, fn)
    denominator = tp + fp + fn
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(100 * precision, 4),
        "recall": round(100 * recall, 4),
        "f_measure": round(100 * f1, 4),
        "jaccard": round(100 * tp / denominator, 4) if denominator else 0.0,
    }


def _key(left, right) -> frozenset:
    return frozenset((str(left).strip(), str(right).strip()))


def load_gold(path: Path, left_col: str, right_col: str) -> tuple[dict, pd.DataFrame]:
    df = pd.read_csv(path)
    for col in (left_col, right_col):
        if col not in df.columns:
            raise ValueError(f"gold file is missing column '{col}'; has {list(df.columns)}")
    if "label" in df.columns:
        df = df[df["label"].astype(str).str.lower() == "duplicate"]
    keys = {_key(r[left_col], r[right_col]): idx for idx, r in df.iterrows()}
    return keys, df


def load_predicted(path: Path, merged_only: bool = True) -> set[frozenset]:
    df = pd.read_csv(path)
    for col in ("first_id", "second_id"):
        if col not in df.columns:
            raise ValueError(
                f"predicted file is missing column '{col}'. Set "
                "join.similarity_config.merge.id_columns in the config so the "
                "merge records which records it paired."
            )
    if merged_only:
        if "merged" in df.columns:
            df = df[df["merged"].astype(str).str.lower().isin(("true", "1"))]
        elif "bucket" in df.columns:
            df = df[df["bucket"].astype(str).str.lower().isin(MERGED_BUCKETS)]
    df = df[df["first_id"].notna() & df["second_id"].notna()]
    return {_key(r["first_id"], r["second_id"]) for _, r in df.iterrows()}


def evaluate(
    gold_path: Path,
    predicted_path: Path,
    left_col: str = "scopus_eid",
    right_col: str = "wos_ut",
    merged_only: bool = True,
    strata: tuple[str, ...] = (),
) -> dict:
    gold_keys, gold_df = load_gold(gold_path, left_col, right_col)
    predicted = load_predicted(predicted_path, merged_only=merged_only)

    gold_set = set(gold_keys)
    tp_keys = predicted & gold_set
    fp_keys = predicted - gold_set
    fn_keys = gold_set - predicted

    report = {
        "overall": _scores(len(tp_keys), len(fp_keys), len(fn_keys)),
        "gold_pairs": len(gold_set),
        "predicted_pairs": len(predicted),
        "strata": {},
    }

    if strata:
        # A false positive has no gold pair, so attribute it to the stratum of
        # whichever of its two records the gold standard does know about.
        record_stratum: dict[str, dict] = {}
        for _, row in gold_df.iterrows():
            attrs = {s: row.get(s) for s in strata}
            record_stratum[str(row[left_col]).strip()] = attrs
            record_stratum[str(row[right_col]).strip()] = attrs

        for stratum in strata:
            if stratum not in gold_df.columns:
                continue
            counts: dict = {}

            def bump(value, field):
                label = "unknown" if pd.isna(value) else str(value)
                counts.setdefault(label, {"tp": 0, "fp": 0, "fn": 0})[field] += 1

            for key in tp_keys | fn_keys:
                row = gold_df.loc[gold_keys[key]]
                bump(row.get(stratum), "tp" if key in tp_keys else "fn")
            for key in fp_keys:
                attrs = next((record_stratum[r] for r in key if r in record_stratum), None)
                bump(attrs.get(stratum) if attrs else None, "fp")

            report["strata"][stratum] = {
                label: _scores(c["tp"], c["fp"], c["fn"]) for label, c in sorted(counts.items())
            }

    return report


def legacy_title_set_scores(
    manual_path: Path,
    automatic_path: Path,
    manual_title_col: str = "Title",
    automatic_title_cols: tuple[str, ...] = ("Title", "Article Title"),
) -> dict:
    """Reproduce the 2024 title-set measure, for comparability with the paper."""
    manual = pd.read_excel(manual_path) if manual_path.suffix != ".csv" else pd.read_csv(manual_path)
    auto = (
        pd.read_excel(automatic_path)
        if automatic_path.suffix != ".csv"
        else pd.read_csv(automatic_path)
    )

    def titles(df: pd.DataFrame, cols) -> set[str]:
        # `_merge_two_sources` lowercases every column name, so the automatic
        # output carries "article title" where the export had "Article Title".
        lookup = {str(c).lower(): c for c in df.columns}
        found = set()
        for col in cols if isinstance(cols, tuple) else (cols,):
            actual = lookup.get(str(col).lower())
            if actual is not None:
                found |= set(df[actual].astype(str).apply(utils.default_process))
        found.discard("")
        found.discard("nan")
        return found

    manual_titles = titles(manual, manual_title_col)
    auto_titles = titles(auto, automatic_title_cols)

    tp = len(auto_titles & manual_titles)
    fp = len(auto_titles - manual_titles)
    fn = len(manual_titles - auto_titles)
    result = _scores(tp, fp, fn)
    result["manual_titles"] = len(manual_titles)
    result["automatic_titles"] = len(auto_titles)
    return result


def format_report(report: dict) -> str:
    lines = []
    overall = report["overall"]
    lines.append(
        f"gold pairs {report['gold_pairs']}   predicted pairs {report['predicted_pairs']}"
    )
    lines.append("")
    lines.append(f"{'':<26}{'TP':>7}{'FP':>7}{'FN':>7}{'P':>9}{'R':>9}{'F':>9}{'Jaccard':>10}")
    lines.append(_row("overall", overall))
    for stratum, groups in report.get("strata", {}).items():
        lines.append("")
        lines.append(f"by {stratum}:")
        for label, scores in groups.items():
            lines.append(_row(f"  {label}", scores))
    return "\n".join(lines)


def _row(label: str, s: dict) -> str:
    return (
        f"{label:<26}{s['tp']:>7}{s['fp']:>7}{s['fn']:>7}"
        f"{s['precision']:>9.2f}{s['recall']:>9.2f}{s['f_measure']:>9.2f}{s['jaccard']:>10.2f}"
    )
