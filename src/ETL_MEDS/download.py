import logging
import os
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig

from .commands import run_command

logger = logging.getLogger(__name__)


def download_file(url, output_dir, session):
    """Download a single file."""
    response = session.get(url, stream=True)
    if response.status_code != 200:
        logger.error(f"Failed to download {url} in streaming download_file get: {response.status_code}")
    response.raise_for_status()

    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path) or "index.html"
    file_path = Path(output_dir) / filename

    with open(file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Downloaded: {file_path}")


def crawl_and_download(base_url, output_dir, session):
    """Recursively crawl and download files."""

    if not base_url.endswith("/"):
        download_file(base_url, output_dir, session)

    response = session.get(base_url)

    if response.status_code != 200:
        logger.error(f"Failed to download {base_url} in initial get: {response.status_code}")
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"]
        full_url = urljoin(base_url, href)
        if not full_url.startswith(base_url):
            continue

        if full_url.endswith("/"):  # It's a directory
            subdir = Path(output_dir) / href.strip("/")
            subdir.mkdir(parents=True, exist_ok=True)
            crawl_and_download(full_url, subdir, session)
        else:
            download_file(full_url, output_dir, session)


def download_data(
    output_dir: Path,
    dataset_info: DictConfig,
    do_demo: bool = False,
    runner_fn: callable = run_command,
):
    """Downloads the data specified in dataset_info.dataset_urls to the output_dir.

    Args:
        output_dir: The directory to download the data to.
        dataset_info: The dataset information containing the URLs to download.
        do_demo: If True, download the demo URLs instead of the main URLs.
        runner_fn: The function to run the command with (added for dependency injection).

    Raises:
        ValueError: If the command fails

    Examples:
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "demo": ["http://example.com/demo"],
        ...         "dataset": ["http://example.com/dataset"],
        ...         "common": ["http://example.com/common"]
        ...     }
        ... })
        >>> def fake_shell_succeed(cmd):
        ...     print(" ".join(cmd))
        >>> def fake_shell_fail(cmd):
        ...     raise ValueError(f"Failed to run {' '.join(cmd)}")
        >>> download_data(Path("data"), cfg, runner_fn=fake_shell_succeed)
        wget -r -N -c -np -nH --directory-prefix data http://example.com/dataset
        wget -r -N -c -np -nH --directory-prefix data http://example.com/common
        >>> download_data(Path("data"), cfg, runner_fn=fake_shell_succeed, do_demo=True)
        wget -r -N -c -np -nH --directory-prefix data http://example.com/demo
        wget -r -N -c -np -nH --directory-prefix data http://example.com/common
        >>> download_data(Path("data"), cfg, runner_fn=fake_shell_fail)
        Traceback (most recent call last):
            ...
        ValueError: Failed to download data from http://example.com/dataset
        >>> cfg = DictConfig({"urls": {"dataset": [{"url": "http://example.com/data", "username": "foo"}]}})
        >>> download_data(Path("data_out"), cfg, runner_fn=fake_shell_succeed)
        wget -r -N -c -np -nH --directory-prefix data_out --user 'foo' http://example.com/data
        >>> cfg = DictConfig({"urls": {"dataset": [{"url": "http://example.com/data", "password": "bar"}]}})
        >>> download_data(Path("data_out"), cfg, runner_fn=fake_shell_succeed)
        wget -r -N -c -np -nH --directory-prefix data_out --password 'bar' http://example.com/data
    """

    if do_demo:
        urls = dataset_info.urls.get("demo", [])
    else:
        urls = dataset_info.urls.get("dataset", [])

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    urls += dataset_info.urls.get("common", [])

    for url in urls:
        session = requests.Session()

        if isinstance(url, (dict, DictConfig)):
            username = url.get("username", None)
            password = url.get("password", None)
            logger.info(f"Authenticating for {username}")
            session.auth = (username, password)
            session.headers.update(
                {
                    "User-Agent": "Wget/1.21.1 (linux-gnu)",
                }
            )

            url = url.url

        try:
            crawl_and_download(url, output_dir, session)
        except ValueError as e:
            raise ValueError(f"Failed to download data from {url}") from e
