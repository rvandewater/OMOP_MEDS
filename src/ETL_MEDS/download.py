from pathlib import Path

from omegaconf import DictConfig

from .commands import run_command


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
        wget -r -N -c -np -nH --directory-prefix data_out --user foo --ask-password http://example.com/data
    """

    if do_demo:
        urls = dataset_info.urls.get("demo", [])
    else:
        urls = dataset_info.urls.get("dataset", [])

    urls += dataset_info.urls.get("common", [])

    for url in urls:
        if isinstance(url, (dict, DictConfig)):
            username = url.get("username", None)
            url = url.url
        else:
            username = None

        command_parts = ["wget -r -N -c -np -nH --directory-prefix", f"{output_dir}"]

        url_parts = url[url.find("://") + 3 :].split("/")[1:]
        if len(url_parts) > 1:
            command_parts.append(f"--cut-dirs {len(url_parts) - 1}")

        if username:
            command_parts.append(f"--user {username} --ask-password")
        command_parts.append(url)

        try:
            runner_fn(command_parts)
        except ValueError as e:
            raise ValueError(f"Failed to download data from {url}") from e
