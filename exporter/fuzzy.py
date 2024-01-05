import logging
import pandas as pd
from rapidfuzz import fuzz, process, utils

from exporter.scripts.context import Config, DataSource
from exporter.types import FuzzyColumnCandidates, MatchesPerColumn

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def suggest():
    config = Config()

    if "fuzzy" not in config.content.get("join", {}).keys():
        logging.info(
            "No fuzzy transformations to apply to the dataset. Are you using a config file that has fuzzy transformations?"
        )
        return

    data = DataSource.get()

    if data.join_sources is None:
        logging.info("Please update config.yml to use a join with multiple datasets.")
        return

    if len(data.join_sources.sources) < 2:
        raise ValueError("Fuzzy join requires at least two sources.")

    df1 = data.join_sources.sources[0].df
    df2 = data.join_sources.sources[1].df

    matches = MatchesPerColumn()
    for col1 in df1.columns:
        print("[df1] Processing column: ", col1)
        for data1 in df1[col1][1:20]:
            for col2 in df2.columns:
                try:
                    score = process.extractOne(
                        data1,
                        df2[col2],
                        scorer=fuzz.QRatio,
                        processor=utils.default_process,
                    )
                    if score[1] < 90:
                        continue

                    matches.column_candidates.setdefault(col1, []).append(
                        FuzzyColumnCandidates(
                            column=col2, reference_data=data1, fuzzy_matches=score
                        )
                    )
                except TypeError:
                    pass
    matched_columns = []
    for col1, candidates in matches.column_candidates.items():
        if len(candidates) == 0:
            continue
        matched_columns.append((col1, candidates[0].column))
    print("Matched columns: ", matched_columns)


def merge():
    columns = [
        {
            "from_": "Title",
            "into_": "Article Title",
            "similarity": {
                "above": 95,
                "cutoff": 85,
                "preprocess": True,
            },
            "is_reference": True,
        },
        {
            "from_": "Authors",
            "into_": "Authors",
            "similarity": {"above": 80, "cutoff": 40, "preprocess": True},
        },
        # {
        #     "from_": "Year",
        #     "into_": "Publication Year",
        #     "similarity": {"above": 100, "cutoff": 100, "preprocess": True},
        # },
        {
            "from_": "DOI",
            "into_": "DOI",
            "similarity": {"above": 100, "cutoff": 100, "preprocess": True},
        },
        {
            "from_": "Source title",
            "into_": "Source Title",
            "similarity": {"above": 95, "cutoff": 90, "preprocess": True},
        },
    ]

    config = {"strategy": "", "drop_duplicates": False}
    data = DataSource.get()

    df1 = data.join_sources.sources[0].df
    df2 = data.join_sources.sources[1].df

    if config.get("drop_duplicates"):
        df1.drop_duplicates(inplace=True)
        df2.drop_duplicates(inplace=True)

    # TODO: validate if columns are present in df1 and df2

    exact_matches = []
    suggested_matches = []
    potential_matches = []
    no_matches = []
    merged_exact_series = []
    merged_suggested_series = []
    # unmerged_potential_series = []
    # unmerged_no_matches_series = []
    common_cols = df1.columns.intersection(df2.columns).tolist()
    print('[df1] Traversing columns: "', columns)
    for _, data2 in df2.iterrows():
        columns_to_score = {}
        for column in columns:
            from_col = column["from_"]
            into_col = column["into_"]
            similarity_above = column["similarity"]["above"]
            similarity_cutoff = column["similarity"]["cutoff"]
            is_reference = column.get("is_reference", False)

            use_processor = (
                utils.default_process
                if (
                    column["similarity"].get("preprocess", None) is None
                    or column["similarity"].get("preprocess", None) is True
                )
                else None
            )
            score = process.extractOne(
                data2[from_col],
                df1[into_col],
                scorer=fuzz.QRatio,
                processor=use_processor,
            )
            columns_to_score.setdefault((from_col, into_col), []).append(score)
            try:
                if score[1] == 100:
                    continue
                if score[1] >= similarity_above:
                    continue
                if score[1] >= similarity_cutoff and not is_reference:
                    continue
                break
            except TypeError:
                continue
        reference_column = [
            (column["from_"], column["into_"])
            for column in columns
            if column.get("is_reference")
        ][0]
        score = columns_to_score[reference_column][0]
        try:
            s1 = data2
            s2 = df1.iloc[score[2]].copy()
            # TODO: make configurable: use col from first, col from second or use both
            # common_cols = s1.index.intersection(s2.index).tolist()
            s1.rename({col: f"{col}_df1" for col in common_cols}, inplace=True)
            s2.rename({col: f"{col}_df2" for col in common_cols}, inplace=True)
            if score[1] == 100:
                exact_matches.append((data2, score))
                res = pd.concat([s1, s2], join="inner")
                merged_exact_series.append(res)
            elif score[1] >= similarity_above:
                suggested_matches.append((data2, score))
                res = pd.concat([s1, s2], join="inner")
                merged_suggested_series.append(res)
            elif score[1] >= similarity_cutoff:
                potential_matches.append((data2, score))
                continue
            elif score[1] < similarity_cutoff:
                no_matches.append((data2, score))
                continue
        except TypeError:
            continue

    print('[df2] Traversing columns: "', columns)
    sorted_exact_matches = sorted(exact_matches, key=lambda x: x[1][2])
    keys_to_match = {t2[2]: (t1, t2) for t1, t2 in sorted_exact_matches}
    skipped = 0
    for idx, data1 in df1.iterrows():
        if idx in keys_to_match.keys():
            skipped += 1
            # already matched
            continue
        columns_to_score = {}
        for column in columns:
            from_col = column["from_"]
            into_col = column["into_"]
            similarity_above = column["similarity"]["above"]
            similarity_cutoff = column["similarity"]["cutoff"]
            is_reference = column.get("is_reference", False)

            use_processor = (
                utils.default_process
                if (
                    column["similarity"].get("preprocess", None) is None
                    or column["similarity"].get("preprocess", None) is True
                )
                else None
            )
            score = process.extractOne(
                data1[into_col],
                df2[from_col],
                scorer=fuzz.QRatio,
                processor=use_processor,
            )

            columns_to_score.setdefault((into_col, from_col), []).append(score)
            try:
                if score[1] == 100:
                    continue
                if score[1] >= similarity_above:
                    continue
                if score[1] >= similarity_cutoff and not is_reference:
                    continue
                break
            except TypeError:
                continue
        reference_column = [
            (column["into_"], column["from_"])
            for column in columns
            if column.get("is_reference")
        ][0]
        score = columns_to_score[reference_column][0]
        try:
            s1 = data1
            s2 = df2.iloc[score[2]].copy()
            # TODO: make configurable: use col from first, col from second or use both
            # common_cols = s1.index.intersection(s2.index).tolist()
            s1.rename({col: f"{col}_df1" for col in common_cols}, inplace=True)
            s2.rename({col: f"{col}_df2" for col in common_cols}, inplace=True)
            if score[1] == 100:
                exact_matches.append((data1, score))
                res = pd.concat([s1, s2], join="inner")
                merged_exact_series.append(res)
            elif score[1] >= similarity_above:
                suggested_matches.append((data1, score))
                res = pd.concat([s1, s2], join="inner")
                merged_suggested_series.append(res)
            elif score[1] >= similarity_cutoff:
                potential_matches.append((data1, score))
                continue
            elif score[1] < similarity_cutoff:
                no_matches.append((data1, score))
                continue
        except TypeError:
            continue
        pass
    merged_exact_df = pd.DataFrame(merged_exact_series)
    merged_suggested_df = pd.DataFrame(merged_suggested_series)
    merged_df = pd.concat([merged_exact_df, merged_suggested_df])

    # exact_matches_series = [t[0] for t in exact_matches]
    # suggested_matches_series = [t[0] for t in suggested_matches]
    potential_matches_series = [t[0] for t in potential_matches]
    no_matches_series = [t[0] for t in no_matches]
    for column in columns:
        from_col = column["from_"]
        into_col = column["into_"]
        for row in (
            # exact_matches_series
            # + suggested_matches_series
            potential_matches_series
            + no_matches_series
        ):
            row.rename({from_col: into_col}, inplace=True)

    # result_series = (
    #     exact_matches_series
    #     + suggested_matches_series
    #     + no_matches_series
    #     + potential_matches_series
    # )
    # result_df = pd.DataFrame(result_series)
    # exact_matches_df = pd.DataFrame(exact_matches_series)
    # suggested_matches_df = pd.DataFrame(suggested_matches_series)
    potential_matches_df = pd.DataFrame(potential_matches_series)
    no_matches_df = pd.DataFrame(no_matches_series)
    no_matches_df = pd.concat([no_matches_df, potential_matches_df])
    breakpoint()
    analytics = {
        "exact_matches": len(exact_matches),
        "suggested_matches": len(suggested_matches),
        "potential_matches": len(potential_matches),
        "no_matches": len(no_matches),
        "sum of merged matches": len(exact_matches) + len(suggested_matches),
        "sum of non-merged matches": len(potential_matches) + len(no_matches),
        "total matches": len(exact_matches)
        + len(suggested_matches)
        + len(potential_matches)
        + len(no_matches),
        "df2 size": len(df2),
        "df1 size": len(df1),
        "merged_df size": len(merged_df),
    }
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(analytics)
    df = merged_df

    print("Saving to file...")
    DataSource.save_to_file(df, Config())

    for df, name in (
        # (exact_matches_df, "exact"),
        # (suggested_matches_df, "suggested"),
        (potential_matches_df, "potential"),
        (no_matches_df, "no"),
    ):
        DataSource.save_to_file(
            df, Config(), name_override=f"config-ftn-{name}-matches.xls"
        )