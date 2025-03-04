import requests
import pandas as pd
import time

from urllib.parse import urlparse

from pathlib import Path
from tesci.scripts.context import Config, DataSource


def download(download_config: Config, dest: Path | None = None):
    url = download_config.get("url")
    if url is None:
        raise ValueError("No URL provided in the download configuration")
    parsed_url = urlparse(url)

    if parsed_url.query is None:
        url += "?cursor=*"
    else:
        url += "&cursor=*" if "cursor=" not in parsed_url.query else ""

    if "mailto" not in parsed_url.query:
        print(
            "Warning: mailto query parameter not found in the URL, API rate limiting may apply.\n"
            "Consider adding your email address to the URL."
        )

    print(f"Downloading data from {url} to {dest}")
    next_cursor = "*"
    df = pd.DataFrame()
    start = time.time()
    while next_cursor is not None:
        url_with_cursor = url.replace("cursor=*", f"cursor={next_cursor}")
        response = requests.get(url_with_cursor)
        if response.status_code != 200:
            raise ValueError(f"Failed to download data from {url_with_cursor}")
        data = response.json()
        df = append_download_response(data, df)
        next_cursor = data["meta"].get("next_cursor", None)
    end = time.time()
    elapsed = end - start
    print("Downloaded and flattened data in {:.2f} seconds".format(elapsed))
    dest = dest or download_config.get("dest")
    if dest is None:
        raise ValueError("Expected destionation path to be provided in the download configuration")
    DataSource.save_to_file(df, Config(), name_override=dest)
    print(f"Data saved to {dest}")


def flatten_json(data: dict, parent_key: str = "", sep: str = ".") -> dict:
    """
    Recursively flattens a nested JSON/dict into a single dict where
    nested keys are separated by `sep`.
    Lists of dictionaries get aggregated so each subkey is pipe-joined
    across all list elements.
    Lists of scalars are also pipe-joined.
    """
    items = {}

    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep=sep))

        elif isinstance(value, list):
            if all(isinstance(elem, dict) for elem in value):
                sub_items = {}
                for elem in value:
                    flattened_elem = flatten_json(elem, "", sep=sep)
                    for subk, subv in flattened_elem.items():
                        sub_items.setdefault(subk, []).append(str(subv))
                # Store pipe-joined strings in items
                # e.g. authorships.raw_author_name => "Name1|Name2|..."
                for subk, subv_list in sub_items.items():
                    items[f"{new_key}.{subk}"] = "|".join(subv_list)
            else:
                items[new_key] = "|".join(map(str, value))
        else:
            items[new_key] = value

    return items


def append_download_response(data: dict, df: pd.DataFrame) -> None:
    """
    Extracts the 'results' portion of the JSON response, flattens each entry,
    and appends it to the given DataFrame in-place.
    """
    # Safety check
    if "results" not in data:
        return

    flattened_rows = []
    for record in data["results"]:
        # Ignore these fields, since they are ignored in the schema
        if "abstract_inverted_index" in record:
            del record["abstract_inverted_index"]
        if "counts_by_year" in record:
            del record["counts_by_year"]
        flattened_record = flatten_json(record)
        flattened_rows.append(flattened_record)

    temp_df = pd.DataFrame(flattened_rows)
    df_len_before = len(df)
    df = pd.concat([df, temp_df], ignore_index=True)

    print(
        f"Appended {len(temp_df)} new rows to the DataFrame "
        f"(size went from {df_len_before} to {len(df)})."
    )
    return df