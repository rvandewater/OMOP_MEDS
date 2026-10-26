import hashlib
import logging
import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# (connect, read) timeouts in seconds. The read timeout bounds each socket read, not the whole transfer,
# so slow-but-progressing downloads still finish while a stalled PhysioNet connection fails instead of
# hanging forever.
REQUEST_TIMEOUT = (30, 300)
# Whole-file attempts for archive downloads; urllib3 retries cannot recover a connection that drops
# mid-body (e.g. ChunkedEncodingError), so the archive download is restarted instead.
ARCHIVE_DOWNLOAD_ATTEMPTS = 3


class MockResponse:  # pragma: no cover
    """A mock requests.Response objects for tests."""

    def __init__(self, status_code: int, contents: str | bytes = ""):
        self.status_code = status_code
        self.contents = contents if isinstance(contents, bytes) else contents.encode()

    def iter_content(self, chunk_size):
        return [
            self.contents[i : i + chunk_size]
            for i in range(0, len(self.contents), chunk_size)
        ]

    @property
    def text(self):
        return self.contents.decode()

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.exceptions.HTTPError(self.status_code)


class MockSession:  # pragma: no cover
    """A mock requests.Session objects for tests."""

    def __init__(
        self,
        return_status: int | dict = 200,
        return_contents: str | bytes | dict = "hello world",
        expect_url: str | None = None,
    ):
        self.return_status = return_status
        self.return_contents = return_contents
        self.expect_url = expect_url
        self.headers = {}
        self.auth = None

    def get(self, url: str, stream: bool = False, timeout=None):
        if self.expect_url is not None and url != self.expect_url:
            raise ValueError(f"Expected URL {self.expect_url}, got {url}")
        if isinstance(self.return_status, dict):
            if url in self.return_status:
                status = self.return_status[url]
            else:
                status = 404
        else:
            status = self.return_status
        if isinstance(self.return_contents, dict):
            if url in self.return_contents:
                contents = self.return_contents[url]
            else:
                status = 404
        else:
            contents = self.return_contents
        return MockResponse(status_code=status, contents=contents)


def download_file(url: str, output_dir: Path, session: requests.Session):
    """Download a single file.

    Args:
        url: The URL to download.
        output_dir: The directory to download the file to.
        session: The requests session to use for downloading.

    Raises:
        Various requests exceptions if the download fails.

    Examples:
        >>> import tempfile
        >>> url = "http://example.com"
        >>> mock_session = MockSession(return_contents="hello world", expect_url=url)
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     download_file(url, Path(tmpdir), mock_session)
        ...     assert len(list(Path(tmpdir).iterdir())) == 1 # Only one file should be downloaded
        ...     out_path = Path(tmpdir) / "index.html"
        ...     out_path.read_text()
        'hello world'
        >>> url = "http://example.com/foo.csv"
        >>> mock_session = MockSession(expect_url=url, return_contents="1,2,3")
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     download_file(url, Path(tmpdir), mock_session)
        ...     assert len(list(Path(tmpdir).iterdir())) == 1 # Only one file should be downloaded
        ...     out_path = Path(tmpdir) / "foo.csv"
        ...     out_path.read_text()
        '1,2,3'
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     download_file("http://example.com", Path(tmpdir), MockSession(return_status=404))
        Traceback (most recent call last):
            ...
        ValueError: Failed to download http://example.com
    """
    try:
        response = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            logger.error(
                f"Failed to download {url} in streaming download_file get: {response.status_code}"
            )
        response.raise_for_status()
    except Exception as e:
        raise ValueError(f"Failed to download {url}") from e

    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path) or "index.html"
    file_path = Path(output_dir) / filename

    with open(file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    logger.info(f"Downloaded: {file_path}")


def crawl_and_download(base_url: str, output_dir: Path, session: requests.Session):
    """Recursively crawl and download files.

    Args:
        base_url: The base URL to crawl.
        output_dir: The directory to download the files to.
        session: The requests session to use for downloading.

    Raises:
        Various requests exceptions if downloads fail.

    Examples:
        >>> import tempfile
        >>> pages = {
        ...     "http://example.com/": (
        ...         "<a href='http://example.com/foo.csv'>foo</a>"
        ...         "<a href='bar/'>bar</a>"
        ...         "<a href='http://example.com/bur/wor.csv'>bur/wor</a>"
        ...         "<div>hello world</div>"
        ...         "<a href='http://example3.com/not_captured.csv'>baz</a>"
        ...     ),
        ...     "http://example.com/foo.csv": "1,2,3,4,5,6",
        ...     "http://example.com/bar/": (
        ...         "<a href='http://example.com/bar/baz.csv'>baz</a>"
        ...         "<a href='http://example.com/bar/qux.csv'>qux</a>"
        ...     ),
        ...     "http://example.com/bar/baz.csv": "7,8,9",
        ...     "http://example.com/bar/qux.csv": "10,11,12",
        ...     "http://example.com/bur/wor.csv": "13,14,15",
        ... }
        >>> mock_session = MockSession(return_contents=pages)
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     crawl_and_download("http://example.com/", tmpdir, mock_session)
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     want = {"foo.csv", "bar/baz.csv", "bar/qux.csv", "bur/wor.csv"}
        ...     assert got == want, f"want {want}, got {got}"
        ...     assert (tmpdir / "foo.csv").read_text() == "1,2,3,4,5,6", "foo.csv check"
        ...     assert (tmpdir / "bar" / "baz.csv").read_text() == "7,8,9", "bar/baz.csv check"
        ...     assert (tmpdir / "bar" / "qux.csv").read_text() == "10,11,12", "bar/qux.csv check"
        ...     assert (tmpdir / "bur" / "wor.csv").read_text() == "13,14,15", "bur/wor.csv check"
    """

    if not base_url.endswith("/"):
        download_file(base_url, output_dir, session)

    try:
        response = session.get(base_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            logger.error(
                f"Failed to download {base_url} in initial get: {response.status_code}"
            )
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise ValueError(f"Failed to download data from {base_url}") from e

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
            filepath = output_dir / full_url.replace(base_url, "")
            subdir = filepath.parent
            subdir.mkdir(parents=True, exist_ok=True)
            download_file(full_url, subdir, session)


def extract_zip(
    zip_fp: Path, output_dir: Path, archive_subdir: str | None = None
) -> int:
    """Extract a zip archive into `output_dir`, optionally keeping only one sub-directory of it.

    With `archive_subdir`, only members below a path component named `archive_subdir` are extracted, and
    they are written relative to that component. This flattens e.g. PhysioNet's
    `<project>-<version>/1_omop_data_csv/person.csv` to `output_dir/person.csv`, the same layout the
    directory crawl produces.

    Args:
        zip_fp: The zip archive to extract.
        output_dir: The directory to extract into.
        archive_subdir: If given, the name of the archive directory whose contents should be extracted.

    Returns:
        The number of files extracted.

    Raises:
        ValueError: If a member would be written outside `output_dir`.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     with zipfile.ZipFile(tmpdir / "a.zip", "w") as zf:
        ...         zf.writestr("proj-0.9/1_omop_data_csv/person.csv", "1,2")
        ...         zf.writestr("proj-0.9/1_omop_data_csv/sub/visit.csv", "3,4")
        ...         zf.writestr("proj-0.9/2_achilles_json/x.json", "{}")
        ...     extract_zip(tmpdir / "a.zip", tmpdir / "out", archive_subdir="1_omop_data_csv")
        ...     sorted(str(f.relative_to(tmpdir / "out")) for f in (tmpdir / "out").rglob("*.*"))
        2
        ['person.csv', 'sub/visit.csv']
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     with zipfile.ZipFile(tmpdir / "a.zip", "w") as zf:
        ...         zf.writestr("../escape.csv", "1,2")
        ...     extract_zip(tmpdir / "a.zip", tmpdir / "out")
        Traceback (most recent call last):
            ...
        ValueError: Refusing to extract ../escape.csv outside of ...
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    root = output_dir.resolve()
    n_extracted = 0
    with zipfile.ZipFile(zip_fp) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            parts = PurePosixPath(member.filename).parts
            if archive_subdir is not None:
                if archive_subdir not in parts:
                    continue
                parts = parts[parts.index(archive_subdir) + 1 :]
            dest = output_dir.joinpath(*parts)
            if not dest.resolve().is_relative_to(root):
                raise ValueError(
                    f"Refusing to extract {member.filename} outside of {output_dir}"
                )
            dest.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst)
            n_extracted += 1
    return n_extracted


def _fetch_to_file(url: str, dest: Path, session: requests.Session, attempts: int):
    """Stream `url` to `dest`, restarting the download on dropped or stalled connections.

    The body is written to `dest` with a `.part` suffix and only renamed into place once complete, so an
    interrupted download never leaves a truncated file at `dest`.
    """
    part_fp = dest.with_name(dest.name + ".part")
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            with open(part_fp, "wb") as file:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    file.write(chunk)
            part_fp.replace(dest)
            return
        except requests.exceptions.HTTPError as e:
            raise ValueError(f"Failed to download {url}") from e
        except requests.exceptions.RequestException as e:
            if attempt == attempts:
                raise ValueError(
                    f"Failed to download {url} after {attempts} attempts"
                ) from e
            wait = 2**attempt
            logger.warning(
                f"Download of {url} failed on attempt {attempt}/{attempts} ({e}); retrying in {wait}s"
            )
            time.sleep(wait)
        finally:
            part_fp.unlink(missing_ok=True)


def download_zip(
    url: str,
    output_dir: Path,
    session: requests.Session,
    archive_subdir: str | None = None,
    attempts: int = ARCHIVE_DOWNLOAD_ATTEMPTS,
    cache_dir: Path | None = None,
    cache_max_age_days: float = 7,
):
    """Download a zip archive and extract it into `output_dir`.

    Dropped or stalled connections restart the download, up to `attempts` times; HTTP error statuses fail
    immediately. Without `cache_dir`, the archive is written to a temporary file inside `output_dir` and
    removed after extraction. With `cache_dir`, the archive is kept there (named by a hash of `url`) and
    reused without downloading while it is younger than `cache_max_age_days`.

    Args:
        url: The URL of the zip archive.
        output_dir: The directory to extract the archive into.
        session: The requests session to use for downloading.
        archive_subdir: If given, only this directory of the archive is extracted (see `extract_zip`).
        attempts: How many times to try the download before giving up.
        cache_dir: If given, the directory in which to cache the downloaded archive.
        cache_max_age_days: How long a cached archive is reused before it is downloaded again.

    Raises:
        ValueError: If the download fails or the archive yields no files.

    Examples:
        >>> import io
        >>> import tempfile
        >>> buf = io.BytesIO()
        >>> with zipfile.ZipFile(buf, "w") as zf:
        ...     zf.writestr("proj-0.9/1_omop_data_csv/2b_person.csv", "1,2,3")
        >>> url = "http://example.com/get-zip/0.9/"
        >>> session = MockSession(return_contents=buf.getvalue(), expect_url=url)
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_zip(url, tmpdir, session, archive_subdir="1_omop_data_csv")
        ...     [str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*")]
        ...     (tmpdir / "2b_person.csv").read_text()
        ['2b_person.csv']
        '1,2,3'
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     download_zip(url, Path(tmpdir), MockSession(return_status=404))
        Traceback (most recent call last):
            ...
        ValueError: Failed to download http://example.com/get-zip/0.9/

    A fresh cached archive is extracted without contacting the server. A stale one is downloaded again,
    falling back to the stale copy if that download fails:
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     offline = MockSession(return_status=404)
        ...     download_zip(url, tmpdir / "a", session, cache_dir=tmpdir / "cache")
        ...     download_zip(url, tmpdir / "b", offline, cache_dir=tmpdir / "cache")
        ...     (tmpdir / "b" / "proj-0.9" / "1_omop_data_csv" / "2b_person.csv").read_text()
        ...     cached_fp, = (tmpdir / "cache").iterdir()
        ...     eight_days_ago = time.time() - 8 * 24 * 3600
        ...     os.utime(cached_fp, (eight_days_ago, eight_days_ago))
        ...     download_zip(url, tmpdir / "c", offline, cache_dir=tmpdir / "cache")
        ...     round((time.time() - cached_fp.stat().st_mtime) / (24 * 3600))
        ...     download_zip(url, tmpdir / "d", session, cache_dir=tmpdir / "cache")
        ...     round((time.time() - cached_fp.stat().st_mtime) / (24 * 3600))
        '1,2,3'
        8
        0
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output_dir) as tmpdir:
        if cache_dir is None:
            zip_fp = Path(tmpdir) / "download.zip"
            _fetch_to_file(url, zip_fp, session, attempts)
        else:
            cache_dir = Path(cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
            zip_fp = cache_dir / f"{hashlib.sha256(url.encode()).hexdigest()[:16]}.zip"
            age_days = (
                (time.time() - zip_fp.stat().st_mtime) / (24 * 3600)
                if zip_fp.is_file()
                else None
            )
            if age_days is not None and age_days < cache_max_age_days:
                logger.info(
                    f"Using cached download of {url} from {zip_fp} ({age_days:.1f} days old)"
                )
            else:
                try:
                    _fetch_to_file(url, zip_fp, session, attempts)
                    logger.info(f"Cached download of {url} at {zip_fp}")
                except ValueError:
                    if age_days is None:
                        raise
                    logger.warning(
                        f"Could not refresh {url}; using the stale cached copy at {zip_fp} "
                        f"({age_days:.1f} days old)"
                    )
        logger.info(f"Extracting {url} into {output_dir}")
        try:
            n_extracted = extract_zip(zip_fp, output_dir, archive_subdir)
        except zipfile.BadZipFile as e:
            if cache_dir is not None:
                zip_fp.unlink(missing_ok=True)
            raise ValueError(f"Downloaded file from {url} is not a zip archive") from e
    if n_extracted == 0:
        raise ValueError(
            f"No files extracted from {url}"
            + (f" under '{archive_subdir}'" if archive_subdir else "")
        )
    logger.info(f"Extracted {n_extracted} files from {url}")


def make_session() -> requests.Session:
    """Create a requests session that retries failed connections and transient server errors."""
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_data(
    output_dir: Path,
    dataset_info: DictConfig,
    do_demo: bool = False,
    session_factory: callable = make_session,
    cache_dir: Path | None = None,
    cache_max_age_days: float = 7,
):
    """Downloads the data specified in dataset_info.dataset_urls to the output_dir.

    Args:
        output_dir: The directory to download the data to.
        dataset_info: The dataset information containing the URLs to download.
        do_demo: If True, download the demo URLs instead of the main URLs.
        session_factory: A callable that returns a requests.Session object (for testing).
        cache_dir: If given, zip archives are cached here and reused (see `download_zip`).
        cache_max_age_days: How long a cached zip archive is reused before it is downloaded again.

    Each URL is either a string, which is crawled (directory listings) or downloaded (single files), or a
    mapping with a `url` key and optional `username`/`password` for authentication. A mapping with
    `format: zip` is downloaded as a zip archive and extracted, keeping only `archive_subdir` if given.

    Raises:
        ValueError: If the command fails

    Examples:
        >>> import tempfile
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "demo": ["http://example.com/demo.csv"],
        ...         "dataset": ["http://example.com/dataset/"],
        ...         "common": ["http://example.com/common.csv"],
        ...     }
        ... })
        >>> demo_session = MockSession(return_contents={
        ...     "http://example.com/demo.csv": "demo", "http://example.com/common.csv": "common"
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=True, session_factory=lambda: demo_session)
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     assert got == {"demo.csv", "common.csv"}, f"want {'demo.csv', 'common.csv'}, got {got}"
        ...     assert (tmpdir / "demo.csv").read_text() == "demo", "demo.csv check"
        ...     assert (tmpdir / "common.csv").read_text() == "common", "common.csv check"
        >>> real_session = MockSession(return_contents={
        ...     "http://example.com/dataset/": "<a href='http://example.com/dataset/foo.csv'>foo</a>",
        ...     "http://example.com/common.csv": "common",
        ...     "http://example.com/dataset/foo.csv": "1,2,3,4,5,6",
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=False, session_factory=lambda: real_session)
        ...     assert real_session.headers == {}, "Headers check"
        ...     assert real_session.auth is None, "Auth check"
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     assert got == {"foo.csv", "common.csv"}, f"want {'foo.csv', 'common.csv'}, got {got}"
        ...     assert len(got) == 2, f"want 2 files, got {[f.relative_to(tmpdir) for f in got]}"
        ...     assert (tmpdir / "foo.csv").read_text() == "1,2,3,4,5,6", "foo.csv check"
        ...     assert (tmpdir / "common.csv").read_text() == "common", "common.csv check"
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "dataset": [{"url": "http://example.com/dataset/", "username": "u", "password": "p"}],
        ...     }
        ... })
        >>> real_session = MockSession(return_contents={
        ...     "http://example.com/dataset/": "<a href='http://example.com/dataset/baz/bar.csv' />",
        ...     "http://example.com/dataset/baz/bar.csv": "1,2,3,4,5,6",
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=False, session_factory=lambda: real_session)
        ...     assert real_session.headers["User-Agent"] == "Wget/1.21.1 (linux-gnu)", "User-Agent check"
        ...     assert real_session.auth == ("u", "p"), "Auth check"
        ...     got = {str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*.*")}
        ...     assert got == {"baz/bar.csv"}, f"want {'baz/bar.csv'}, got {got}"
        ...     assert (tmpdir / "baz/bar.csv").read_text() == "1,2,3,4,5,6", "foo.csv check"

    Zip archives are extracted, and authentication is only set up when a username is given:
        >>> import io, zipfile
        >>> buf = io.BytesIO()
        >>> with zipfile.ZipFile(buf, "w") as zf:
        ...     zf.writestr("proj/1_omop_data_csv/2b_person.csv", "1,2,3")
        ...     zf.writestr("proj/2_achilles_json/x.json", "{}")
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "demo": [{"url": "http://example.com/get-zip/", "format": "zip",
        ...                   "archive_subdir": "1_omop_data_csv"}],
        ...     }
        ... })
        >>> zip_session = MockSession(return_contents={"http://example.com/get-zip/": buf.getvalue()})
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=True, session_factory=lambda: zip_session)
        ...     assert zip_session.auth is None, "Auth check"
        ...     [str(f.relative_to(tmpdir)) for f in tmpdir.rglob("*")]
        ['2b_person.csv']

    If the internal download fails, a ValueError is raised:
        >>> cfg = DictConfig({
        ...     "urls": {
        ...         "demo": ["http://example.com/demo.csv"],
        ...         "dataset": ["http://example.com/dataset/"],
        ...         "common": ["http://example.com/common.csv"],
        ...     }
        ... })
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     tmpdir = Path(tmpdir)
        ...     download_data(tmpdir, cfg, do_demo=True, session_factory=lambda: real_session)
        Traceback (most recent call last):
            ...
        ValueError: Failed to download data from http://example.com/demo.csv
    """

    if do_demo:
        urls = dataset_info.urls.get("demo", [])
    else:
        urls = dataset_info.urls.get("dataset", [])

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    urls += dataset_info.urls.get("common", [])

    for url in urls:
        session = session_factory()

        url_format = None
        archive_subdir = None
        if isinstance(url, (dict, DictConfig)):
            username = url.get("username", None)
            password = url.get("password", None)
            if username is not None:
                logger.info(f"Authenticating for {username}")
                session.auth = (username, password)
                session.headers.update({"User-Agent": "Wget/1.21.1 (linux-gnu)"})
            url_format = url.get("format", None)
            archive_subdir = url.get("archive_subdir", None)

            url = url.url

        try:
            if url_format == "zip":
                download_zip(
                    url,
                    output_dir,
                    session,
                    archive_subdir=archive_subdir,
                    cache_dir=cache_dir,
                    cache_max_age_days=cache_max_age_days,
                )
            elif url_format is None:
                crawl_and_download(url, output_dir, session)
            else:
                raise ValueError(
                    f"Unsupported download format '{url_format}' for {url}; expected 'zip' or no format"
                )
        except ValueError as e:
            raise ValueError(f"Failed to download data from {url}") from e
