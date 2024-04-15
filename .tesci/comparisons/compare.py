import pandas as pd
from pathlib import Path
from rapidfuzz import utils
import argparse

argparse = argparse.ArgumentParser()
argparse.add_argument("--manual", type=str, required=True)
argparse.add_argument("--automatic", type=str, required=True)
args = argparse.parse_args()


def calculate_results(true_positive, false_positive, false_negative):
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


def compare_results(df1, df2):
    titles_df1 = set(df1["Title"].astype(str).apply(utils.default_process)) # | set(df1["Article Title"].astype(str).apply(utils.default_process))
    titles_df2 = set(df2["title"].astype(str).apply(utils.default_process)) # | set(df2["Article Title"].astype(str).apply(utils.default_process))
    # titles_df1_processed = set(df1["Title"].apply(utils.default_process))
    # titles_df2_processed = set(df2["Title"].apply(utils.default_process))
    # duplicates_in_df2 = df2[df2.duplicated(subset="Title", keep="first")]
    # True Positives (TP) - Titles in df2 that are also in df1
    true_positives = titles_df2.intersection(titles_df1)
    # False Positives (FP) - Titles in df2 that are not in df1
    false_positives = titles_df2.difference(titles_df1)
    # False Negatives (FN) - Titles in df1 that are not in df2
    false_negatives = titles_df1.difference(
        (set(df2["Title"].apply(utils.default_process)) | set(df2["Article Title"].apply(utils.default_process)))
    )
    tp = len(true_positives)
    fp = len(false_positives)
    fn = len(false_negatives)

    precision, recall, f1 = calculate_results(tp, fp, fn)
    print(f"Precision: {precision}, Recall: {recall}, F1: {f1}")

    jaccard_index = tp / (tp + fp + fn)
    print(f"Jaccard: {jaccard_index}")

    return precision, recall, f1, jaccard_index


def main():
    df1 = pd.read_excel(Path(args.manual))
    df2 = pd.read_excel(Path(args.automatic))
    breakpoint()
    precision, recall, f1, jaccard = compare_results(df1, df2)
    # df1 = pd.read_excel(Path("ftn-manual-notmerged-bibliometrija.xlsx"))
    # df2 = pd.read_excel(Path("config-ftn-no_matches_to_compare-matches.xls"))

    # diffs = pd.concat([df1, df2]).loc[df1.index.symmetric_difference(df2.index)]
    # print(diffs)
    # diffs.to_excel(Path("diffs.xlsx"), index=False)
    # df = diffs.merge(
    #     df1.drop_duplicates(), on=["Article Title"], how="left", indicator=True
    # )
    # not_in_manual_df = df[df["_merge"] == "left_only"]
    # not_in_manual_df.to_excel(Path("not-in-manual-2.xlsx"), index=False)

    df = pd.merge(df1, df2, on=["Title"], how="left", indicator=True)
    not_in_automatic_df = df[df["_merge"] == "left_only"]
    not_in_automatic_df.to_excel(Path("not-in-automatic.xlsx"), index=False)


if __name__ == "__main__":
    main()
