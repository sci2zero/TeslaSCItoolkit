from tesci.api.providers import AVAILABLE_PROVIDERS

from pathlib import Path
from tesci.scripts.context import Config


def download(dest: Path | None = None):
    """
    Download the data from the provider and save it to the destination
    """
    config = Config()
    download_config = config.content.get("download")
    if download_config is None:
        raise ValueError("No download configuration found in the config file")

    provider = download_config.get("provider")
    match provider:
        case "openalex":
            import tesci.api.providers.openalex as openalex

            openalex.download(download_config, dest)
        case _:
            raise NotImplementedError(
                f"Provider {provider} not supported. Available providers are: {AVAILABLE_PROVIDERS}"
            )
