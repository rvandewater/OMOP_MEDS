from pathlib import Path

from OmegaConf import DictConfig

from .commands import run_command


def download_data(output_dir: Path, dataset_info: DictConfig, do_demo: bool = False):
    """Downloads the data specified in dataset_info.dataset_urls to the output_dir.

    Args:
        output_dir: The directory to download the data to.
        dataset_info: The dataset information containing the URLs to download.
        do_demo: If True, download the demo URLs instead of the main URLs.
    """

    command_parts = ["wget -r -N -c -np --directory-prefix", f"{output_dir}"]

    if do_demo:
        urls = dataset_info.urls.get("demo", [])
    else:
        urls = dataset_info.urls.get("dataset", [])

    urls += dataset_info.urls.get("common", [])

    for url in urls:
        if isinstance(url, dict):
            url = url.url
            username = url.get("username", None)
        else:
            username = None

        if username:
            command_parts.append(f"--user {username} --ask-password")
        command_parts.append(url)

        try:
            run_command(command_parts)
        except ValueError as e:
            raise ValueError(f"Failed to download data from {url}") from e
