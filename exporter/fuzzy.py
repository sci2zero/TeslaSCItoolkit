from enum import Enum
import logging
import pandas as pd
from rapidfuzz import fuzz, process, utils

from exporter.scripts.context import Config, DataSource
from exporter.types import FuzzyColumnCandidates, MatchesPerColumn

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


class MergeState(Enum):
    EXACT = 0
    SUGGESTED = 1
    POTENTIAL = 2
    NO_MATCH = 3


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


def _get_reference_column(columns):
    return next(filter(lambda col: col.get("is_reference", False), columns), None)


def _preprocess_data(df1, df2, from_col, into_col, preprocess_config):
    remove = preprocess_config.get("remove", None)
    replace = preprocess_config.get("replace", None)
    truncate_after = preprocess_config.get("truncate_after", None)
    if remove is not None:
        pass
    if replace is not None:
        df2[from_col] = df2[from_col].replace(replace)
    if truncate_after is not None:
        for truncate_str in truncate_after:
            df2[from_col] = df2[from_col].str.split(truncate_str).str[0]
    pass


def merge():
    columns = [
        {
            "from_": "Title",
            "into_": "Article Title",
            "similarity": {
                "above": 90,
                "cutoff": 45,
                "preprocess": True,
            },
            "is_reference": True,
            "preprocess": {
                # "remove": ["the", "a", "an", "of", "in", "on", "for", "and", "or", "to"], # todo: remove
                "truncate_after": ["; "]
            }
        },
        {
            "from_": "Authors",
            "into_": "Authors",
            "similarity": {"above": 40, "cutoff": 30, "preprocess": True},
        },
        {
            "from_": "Year",
            "into_": "Publication Year",
            "similarity": {"above": 100, "cutoff": 100, "preprocess": True},
        },
        {
            "from_": "DOI",
            "into_": "DOI",
            "similarity": {"above": 100, "cutoff": 100, "preprocess": True},
        },
        # {
        #     "from_": "Source title",
        #     "into_": "Source Title",
        #     "similarity": {"above": 20, "cutoff": 10, "preprocess": True},
        # },
        # {
        #     "from_": "Document Type",
        #     "into_": "Document Type",
        #     "similarity": {
        #         "above": 100,
        #         "cutoff": 100,
        #         "preprocess": True,
        #     },
        #     "preprocess": {
        #         "replace": {
        #             "Conference paper": "Proceedings Paper",
        #             "Editorial": "Editorial Material",
        #             "Article in press": "Article; Early Access",
        #             "Book chapter": "Book Review",
        #         }
        #     },
        # },
    ]

    config = {"strategy": "", "drop_duplicates": False}
    data = DataSource.get()

    df1 = data.join_sources.sources[0].df
    df2 = data.join_sources.sources[1].df

    if config.get("drop_duplicates"):
        df1.drop_duplicates(inplace=True)
        df2.drop_duplicates(inplace=True)

    for column in columns:
        from_col = column["from_"]
        into_col = column["into_"]
        similarity = column["similarity"]
        if similarity.get("preprocess", None) is None:
            continue
        if similarity.get("preprocess", None) is True:
            df1[into_col] = df1[into_col].astype(str)
            df2[from_col] = df2[from_col].astype(str)
        if column.get("preprocess", None) is not None:
            preprocess_config = column["preprocess"]
            _preprocess_data(df1, df2, from_col, into_col, preprocess_config)

    # TODO: validate if columns are present in df1 and df2
    # TODO: auto-determine if column can be nullable, this useful for DOI or Year columns where 100% match is expected
    exact_matches = []
    suggested_matches = []
    potential_matches = []
    no_matches = []
    merged_exact_series = []
    merged_suggested_series = []
    common_cols = df1.columns.intersection(df2.columns).tolist()
    print('[df1] Traversing columns: "', columns)
    reference_column = _get_reference_column(columns)
    for _, data2 in df2.iterrows():
        row_states = []  # [exact, suggested, potential, no]
        score = process.extractOne(
            data2[reference_column["from_"]],
            df1[reference_column["into_"]],
            scorer=fuzz.QRatio,
            processor=utils.default_process,
        )
        s2 = df1.iloc[score[2]].copy()
        for column in columns:
            from_col = column["from_"]
            into_col = column["into_"]
            similarity_above = column["similarity"]["above"]
            similarity_cutoff = column["similarity"]["cutoff"]
            if (
                pd.isna(data2[from_col])
                or data2[from_col] == "nan"
                or pd.isna(s2[into_col])
                or s2[into_col] == "nan"
            ):
                continue

            ratio = fuzz.QRatio(
                data2[from_col], s2[into_col], processor=utils.default_process
            )
            if score[1] == 100 and ratio < 100 and from_col == "Document Type":
                breakpoint()
            if ratio == 100:
                row_states.append(MergeState.EXACT)
                continue
            if ratio >= similarity_above:
                row_states.append(MergeState.SUGGESTED)
                continue
            if ratio >= similarity_cutoff:
                row_states.append(MergeState.POTENTIAL)
                continue
            row_states.append(MergeState.NO_MATCH)

        try:
            s1 = data2
            if MergeState.NO_MATCH in row_states:
                no_matches.append((data2, score))
                continue
            if MergeState.POTENTIAL in row_states:
                potential_matches.append((data2, score))
                continue

            s1.rename({col: f"{col}_df1" for col in common_cols}, inplace=True)
            s2.rename({col: f"{col}_df2" for col in common_cols}, inplace=True)

            if MergeState.SUGGESTED in row_states:
                suggested_matches.append((data2, score))
                res = pd.concat([s1, s2], join="inner")
                merged_suggested_series.append(res)
                continue

            exact_matches.append((data2, score))
            res = pd.concat([s1, s2], join="inner")
            merged_exact_series.append(res)
        except TypeError:
            continue

    print('[df2] Traversing columns: "', columns)
    sorted_exact_matches = sorted(exact_matches, key=lambda x: x[1][2])
    sorted_suggested_matches = sorted(suggested_matches, key=lambda x: x[1][2])

    keys_to_match = {
        t2[2]: (t1, t2)
        for t1, t2 in (
            sorted_exact_matches
            + sorted_suggested_matches
            # + sorted_potential_matches
            # + sorted_no_matches
        )
    }
    skipped = 0
    for idx, data1 in df1.iterrows():
        if idx in keys_to_match.keys():
            skipped += 1
            # already matched
            continue
        row_states = []  # [exact, suggested, potential, no]
        score = process.extractOne(
            data1[reference_column["into_"]],
            df2[reference_column["from_"]],
            scorer=fuzz.QRatio,
            processor=utils.default_process,
        )
        s2 = df2.iloc[score[2]].copy()
        for column in columns:
            from_col = column["from_"]
            into_col = column["into_"]
            similarity_above = column["similarity"]["above"]
            similarity_cutoff = column["similarity"]["cutoff"]
            if (
                pd.isna(data1[into_col])
                or data1[into_col] == "nan"
                or pd.isna(s2[from_col])
                or s2[from_col] == "nan"
            ):
                continue

            ratio = fuzz.QRatio(
                data1[into_col], s2[from_col], processor=utils.default_process
            )
            if ratio == 100:
                row_states.append(MergeState.EXACT)
                continue
            if ratio >= similarity_above:
                row_states.append(MergeState.SUGGESTED)
                continue
            if ratio >= similarity_cutoff:
                row_states.append(MergeState.POTENTIAL)
                continue
            row_states.append(MergeState.NO_MATCH)

        try:
            s1 = data1
            s1.rename({col: f"{col}_df1" for col in common_cols}, inplace=True)
            s2.rename({col: f"{col}_df2" for col in common_cols}, inplace=True)

            if MergeState.NO_MATCH in row_states:
                no_matches.append((data1, score))
                continue
            if MergeState.POTENTIAL in row_states:
                potential_matches.append((data1, score))
                continue

            s1.rename({col: f"{col}_df1" for col in common_cols}, inplace=True)
            s2.rename({col: f"{col}_df2" for col in common_cols}, inplace=True)

            if MergeState.SUGGESTED in row_states:
                suggested_matches.append((data1, score))
                res = pd.concat([s1, s2], join="inner")
                merged_suggested_series.append(res)
                continue

            exact_matches.append((data1, score))
            res = pd.concat([s1, s2], join="inner")
            merged_exact_series.append(res)
        except TypeError:
            continue
        pass
    merged_exact_df = pd.DataFrame(merged_exact_series)
    merged_suggested_df = pd.DataFrame(merged_suggested_series)
    merged_df = pd.concat([merged_exact_df, merged_suggested_df])

    exact_matches_series = [t[0] for t in exact_matches]
    suggested_matches_series = [t[0] for t in suggested_matches]
    potential_matches_series = [t[0] for t in potential_matches]
    no_matches_series = [t[0] for t in no_matches]
    for column in columns:
        from_col = column["from_"]
        into_col = column["into_"]
        for row in (
            exact_matches_series
            + suggested_matches_series
            + potential_matches_series
            + no_matches_series
        ):
            row.rename({from_col: into_col}, inplace=True)

    exact_matches_df = pd.DataFrame(exact_matches_series)
    suggested_matches_df = pd.DataFrame(suggested_matches_series)
    potential_matches_df = pd.DataFrame(potential_matches_series)
    no_matches_df = pd.DataFrame(no_matches_series)
    no_matches_df = pd.concat([no_matches_df, potential_matches_df])

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
        "duplicates in merged_df": len(
            merged_df[merged_df.duplicated(subset="Title", keep="first")]
        ),
    }
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(analytics)
    breakpoint()
    df = merged_df

    print("Saving to file...")
    DataSource.save_to_file(df, Config())

    for df, name in (
        (exact_matches_df, "exact"),
        (suggested_matches_df, "suggested"),
        (potential_matches_df, "potential"),
        (no_matches_df, "no"),
    ):
        DataSource.save_to_file(
            df, Config(), name_override=f"config-ftn-{name}-matches.xls"
        )
