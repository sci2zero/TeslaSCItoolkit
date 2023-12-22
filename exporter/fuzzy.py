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
                    if score[1] < 80:
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
    # columns:
    # - from: "Authors"
    #   into: "Authors"
    #   ratio:
    #     above: 75
    #     cutoff: 55
    # - from: "Article Title"
    #   into: "Title"
    #   ratio:
    #     above: 90
    #     cutoff: 80
    columns = [
        # {"from_": "Authors", "into_": "Authors", "ratio": {"above": 75, "cutoff": 55}},
        {
            "from_": "Title",
            "into_": "Article Title",
            "ratio": {"above": 90, "cutoff": 80},
        },
    ]
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

    for column in columns:
        from_col = column["from_"]
        into_col = column["into_"]
        ratio_above = column["ratio"]["above"]
        ratio_cutoff = column["ratio"]["cutoff"]

        print('[df1] Merging column: "', from_col, '" with column: "', into_col + '"')
        for data2 in df2[from_col]:
            score = process.extractOne(
                data2,
                df1[into_col],
                scorer=fuzz.WRatio,
                processor=utils.default_process,
            )
            try:
                if score[1] == 100:
                    exact_matches.append((data2, score))
                    continue
                elif score[1] >= ratio_above:
                    suggested_matches.append((data2, score))
                elif score[1] >= ratio_cutoff:
                    potential_matches.append((data2, score))
                elif score[1] < ratio_cutoff:
                    no_matches.append((data2, score))
            except TypeError:
                continue
            # using pandas, create a new dataframe which contains merged data from df1 and df2.
            # the criteria for merging is a shared column with a fuzz ratio above 80
            if score[1] >= ratio_above:
                breakpoint()
                result_df[into_col] = result_df[into_col].replace(score[0], data2)
                # TODO: cascade other columns for that exact match into df1
    # breakpoint()
    pass
