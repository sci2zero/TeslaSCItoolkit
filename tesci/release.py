import datetime
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from enum import Enum
from github import Auth, Github

from tesci.scripts.context import Config


class Strategy(Enum):
    """Strategy for releasing the exported data."""

    COMMIT = "commit"
    TAG = "tag"


def release(strategy: Strategy | None) -> None:
    config = Config()
    releases = config.content["release"]
    destination_path = config.destination_path
    _release_to_remote_batch(releases, destination_path, strategy)


def _release_to_remote_batch(
    releases: list[dict[str, Any]], destination_path: Path, strategy: Strategy | None
) -> None:
    if strategy is not None:
        releases = [release for release in releases if release["strategy"] == strategy]

    for release in releases:
        _release_to_remote(release, destination_path)


def _release_to_remote(release: dict[str, Any], destination_path: Path) -> None:
    repo_url = release["url"]
    print(f"Releasing {destination_path.name} to {repo_url}")
    parsed_url = urlparse(repo_url)
    path_parts = parsed_url.path.split("/")
    org_and_name = f"{path_parts[1]}/{path_parts[2]}"
    token = os.environ.get("RELEASE_API_TOKEN") or release["token"]
    file_name = destination_path.name

    auth = Auth.Token(token)
    repo = Github(auth=auth).get_repo(org_and_name)

    try:
        with open(destination_path, "rb") as dest_file:
            content = dest_file.read()
            commit_message = "`{}`, {}".format(
                file_name, datetime.datetime.today().replace(microsecond=0)
            )
            try:
                result = repo.get_contents(path=file_name)
            except Exception:
                # file does not exist
                result = None

            if result is not None:
                # if file exists, then call update instead of create
                repo.update_file(
                    path=file_name,
                    message=commit_message,
                    content=content,
                    sha=result.sha,
                )
            else:
                repo.create_file(
                    path=file_name, message=commit_message, content=content
                )
    except Exception as e:
        print(f"Failed to release {file_name} to {repo_url}")
        print(e)
        return
    print(f"Released {file_name} to {repo_url}")
