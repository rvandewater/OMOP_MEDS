import io
import zipfile
from pathlib import Path

import pytest
import requests

from OMOP_MEDS import download
from OMOP_MEDS.download import REQUEST_TIMEOUT, download_zip

URL = "https://example.com/get-zip/0.9/"


def _zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("proj-0.9/1_omop_data_csv/2b_person.csv", "person_id\n1\n")
        zf.writestr(
            "proj-0.9/1_omop_data_csv/2b_visit_occurrence.csv",
            "visit_occurrence_id\n1\n",
        )
        zf.writestr("proj-0.9/2_achilles_json/report.json", "{}")
    return buf.getvalue()


class _Response:
    def __init__(self, body: bytes, fail_after: int | None = None):
        self.body = body
        self.fail_after = fail_after

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        # Mimics PhysioNet dropping the connection part-way through the body, which surfaces from
        # requests as ChunkedEncodingError while iterating rather than from session.get().
        if self.fail_after is not None:
            yield self.body[: self.fail_after]
            raise requests.exceptions.ChunkedEncodingError(
                "Connection broken: IncompleteRead"
            )
        yield self.body


class _FlakySession:
    """Fails the first `n_failures` downloads mid-body, then succeeds."""

    def __init__(self, n_failures: int):
        self.n_failures = n_failures
        self.calls = []

    def get(self, url, stream=False, timeout=None):
        self.calls.append({"url": url, "stream": stream, "timeout": timeout})
        body = _zip_bytes()
        if len(self.calls) <= self.n_failures:
            return _Response(body, fail_after=len(body) // 3)
        return _Response(body)


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(download.time, "sleep", lambda seconds: None)


def test_retries_dropped_connection_and_extracts_only_csv_dir(tmp_path: Path):
    session = _FlakySession(n_failures=2)

    download_zip(URL, tmp_path / "raw", session, archive_subdir="1_omop_data_csv")

    assert len(session.calls) == 3
    assert all(c["timeout"] == REQUEST_TIMEOUT and c["stream"] for c in session.calls)
    assert sorted(p.name for p in (tmp_path / "raw").iterdir()) == [
        "2b_person.csv",
        "2b_visit_occurrence.csv",
    ]
    assert (tmp_path / "raw" / "2b_person.csv").read_text() == "person_id\n1\n"


def test_gives_up_after_all_attempts_without_leaving_partial_files(tmp_path: Path):
    session = _FlakySession(n_failures=10)
    cache_dir = tmp_path / "cache"

    with pytest.raises(ValueError, match="after 3 attempts"):
        download_zip(URL, tmp_path / "raw", session, cache_dir=cache_dir)

    assert len(session.calls) == 3
    assert list(cache_dir.iterdir()) == []
    assert list((tmp_path / "raw").iterdir()) == []


def test_cache_is_reused_within_max_age(tmp_path: Path):
    cache_dir = tmp_path / "cache"
    first = _FlakySession(n_failures=0)
    download_zip(
        URL,
        tmp_path / "a",
        first,
        archive_subdir="1_omop_data_csv",
        cache_dir=cache_dir,
    )

    second = _FlakySession(n_failures=0)
    download_zip(
        URL,
        tmp_path / "b",
        second,
        archive_subdir="1_omop_data_csv",
        cache_dir=cache_dir,
    )

    assert len(first.calls) == 1
    assert second.calls == []
    assert len(list(cache_dir.glob("*.zip"))) == 1
    assert (tmp_path / "b" / "2b_person.csv").read_text() == "person_id\n1\n"


def test_cache_max_age_zero_always_downloads(tmp_path: Path):
    cache_dir = tmp_path / "cache"
    for out in ("a", "b"):
        session = _FlakySession(n_failures=0)
        download_zip(
            URL, tmp_path / out, session, cache_dir=cache_dir, cache_max_age_days=0
        )
        assert len(session.calls) == 1


def test_corrupt_cached_archive_is_discarded(tmp_path: Path):
    cache_dir = tmp_path / "cache"
    download_zip(URL, tmp_path / "a", _FlakySession(n_failures=0), cache_dir=cache_dir)
    (cached_fp,) = cache_dir.glob("*.zip")
    cached_fp.write_bytes(b"not a zip")

    with pytest.raises(ValueError, match="not a zip archive"):
        download_zip(
            URL, tmp_path / "b", _FlakySession(n_failures=0), cache_dir=cache_dir
        )

    assert not cached_fp.exists()
    session = _FlakySession(n_failures=0)
    download_zip(URL, tmp_path / "c", session, cache_dir=cache_dir)
    assert len(session.calls) == 1


def test_missing_archive_subdir_fails_loudly(tmp_path: Path):
    with pytest.raises(ValueError, match="No files extracted .* under 'no_such_dir'"):
        download_zip(
            URL,
            tmp_path / "raw",
            _FlakySession(n_failures=0),
            archive_subdir="no_such_dir",
        )
