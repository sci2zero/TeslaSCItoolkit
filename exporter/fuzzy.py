import logging

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
                "above": 99,
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
    ]

    config = {"strategy": ""}  # TODO
    data = DataSource.get()

    df1 = data.join_sources.sources[0].df
    df2 = data.join_sources.sources[1].df

    potential_matches = []

    # TODO: validate if columns are present in df1 and df2

    exact_matches = []
    suggested_matches = []
    potential_matches = []
    no_matches = []

    result_df = df1.copy()
    print('[df1] Traversing columns: "', columns)

    for data2 in df2.iterrows():
        columns_to_score = {}
        can_merge = True
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
                data2[1][from_col],
                result_df[into_col],
                scorer=fuzz.QRatio,
                processor=use_processor,
            )

            columns_to_score.setdefault((from_col, into_col), []).append(score)

            if score[1] == 100:
                continue
            if score[1] >= similarity_above:
                continue
            if score[1] >= similarity_cutoff and not is_reference:
                continue
            can_merge = False
            break

        if can_merge:
            for matched_columns, scores in columns_to_score.items():
                from_col, into_col = matched_columns
                result_df[into_col] = result_df[into_col].replace(
                    scores[0], data2[1][from_col]
                )
        reference_column = [
            (column["from_"], column["into_"])
            for column in columns
            if column.get("is_reference")
        ][0]
        score = columns_to_score[reference_column][0]
        try:
            if score[1] == 100:
                breakpoint()
                exact_matches.append((data2, score))
            elif score[1] >= similarity_above:
                suggested_matches.append((data2, score))
            elif score[1] >= similarity_cutoff:
                potential_matches.append((data2, score))
                continue
            elif score[1] < similarity_cutoff:
                no_matches.append((data2, score))
                continue
        except TypeError:
            continue
    into_columns = [column["into_"] for column in columns if column["into_"]]
    from_columns = [column["from_"] for column in columns if column["from_"]]
    result_df = result_df.merge(
        df2,
        how="left",
        left_on=into_columns,
        right_on=from_columns,
        suffixes=("", "_df2"),
    )
    result_df.drop(result_df.filter(regex="_df2$").columns, axis=1, inplace=True)
    result_df = result_df.drop_duplicates()
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
        "result_df size": len(result_df),
    }
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(analytics)
    df = result_df
    breakpoint()
    DataSource.save_to_file(df, Config())
