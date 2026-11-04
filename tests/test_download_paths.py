"""Failure and logging paths of `OMOP_MEDS.download`, exercised with fake sessions (no network)."""

import logging
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from omegaconf import DictConfig

from OMOP_MEDS import download
from OMOP_MEDS.download import (
    MockSession,
    _fetch_to_file,
    crawl_and_download,
    download_data,
    extract_zip,
)

URL = "https://example.com/archive.zip"


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    # Patch only download.py's `time` reference so retries do not sleep; pytest's own clock is untouched.
    monkeypatch.setattr(
        download,
        "time",
        SimpleNamespace(
            monotonic=time.monotonic, time=time.time, sleep=lambda seconds: None
        ),
    )


class _ChunkedResponse:
    def __init__(self, chunks: list[bytes], content_length: int):
        self.chunks = chunks
        self.headers = {"Content-Length": str(content_length)}
        self.status_code = 200

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield from self.chunks


class _ChunkedSession:
    def __init__(self, chunks: list[bytes], content_length: int):
        self.chunks, self.content_length = chunks, content_length
        self.calls = 0

    def get(self, url, stream=False, timeout=None, headers=None):
        self.calls += 1
        return _ChunkedResponse(self.chunks, self.content_length)


def test_crawl_reports_and_raises_on_http_error(tmp_path: Path, caplog):
    with caplog.at_level(logging.ERROR, logger="OMOP_MEDS.download"):
        with pytest.raises(ValueError, match="Failed to download data from http://x/"):
            crawl_and_download("http://x/", tmp_path, MockSession(return_status=404))

    assert any("in initial get: 404" in r.message for r in caplog.records)
    assert list(tmp_path.iterdir()) == []


def test_extract_zip_skips_directory_entries(tmp_path: Path):
    zip_fp = tmp_path / "a.zip"
    with zipfile.ZipFile(zip_fp, "w") as zf:
        zf.writestr(zipfile.ZipInfo("proj/1_omop_data_csv/"), "")
        zf.writestr("proj/1_omop_data_csv/person.csv", "person_id\n1\n")

    n = extract_zip(zip_fp, tmp_path / "out", archive_subdir="1_omop_data_csv")

    assert n == 1
    assert [p.name for p in (tmp_path / "out").iterdir()] == ["person.csv"]


def test_fetch_logs_progress_every_progress_interval(
    tmp_path: Path, caplog, monkeypatch
):
    monkeypatch.setattr(download, "PROGRESS_LOG_BYTES", 8)
    chunks = [b"a" * 10, b"b" * 10, b"c" * 10]
    dest = tmp_path / "archive.zip"

    with caplog.at_level(logging.INFO, logger="OMOP_MEDS.download"):
        _fetch_to_file(URL, dest, _ChunkedSession(chunks, 30), attempts=1)

    assert dest.read_bytes() == b"".join(chunks)
    progress = [
        r.message for r in caplog.records if r.message.startswith("Downloading ")
    ]
    # 10, 20 and 30 bytes each cross the next 8-byte mark.
    assert len(progress) == 3
    assert all(f"Downloading {URL}: " in m for m in progress)


def test_fetch_retries_a_truncated_body_then_fails(tmp_path: Path):
    # The server announces 30 bytes but closes after 20, on every attempt.
    session = _ChunkedSession([b"x" * 20], 30)
    dest = tmp_path / "archive.zip"

    with pytest.raises(ValueError, match="after 2 attempts") as excinfo:
        _fetch_to_file(URL, dest, session, attempts=1)

    assert isinstance(excinfo.value.__cause__, requests.exceptions.ConnectionError)
    assert "ended after 20 of 30 bytes" in str(excinfo.value.__cause__)
    # The first attempt made progress, so it does not count against `attempts`; the second did not.
    assert session.calls == 2
    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()


def test_download_data_rejects_unknown_format(tmp_path: Path):
    cfg = DictConfig(
        {
            "urls": {
                "demo": [{"url": "http://x/demo.tar", "format": "tar"}],
                "common": [],
            }
        }
    )

    with pytest.raises(
        ValueError, match="Failed to download data from http://x/demo.tar"
    ) as excinfo:
        download_data(tmp_path, cfg, do_demo=True, session_factory=MockSession)

    assert "Unsupported download format 'tar'" in str(excinfo.value.__cause__)
