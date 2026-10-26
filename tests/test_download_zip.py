import io
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from OMOP_MEDS import download
from OMOP_MEDS.download import (
    REQUEST_TIMEOUT,
    DownloadBudgetExceeded,
    _fetch_to_file,
    download_zip,
    make_session,
)

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
        self.headers = {"Content-Length": str(len(body))}
        self.status_code = 200

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

    def get(self, url, stream=False, timeout=None, headers=None):
        self.calls.append({"url": url, "stream": stream, "timeout": timeout})
        body = _zip_bytes()
        if len(self.calls) <= self.n_failures:
            return _Response(body, fail_after=len(body) // 3)
        return _Response(body)


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    # Replace only download.py's `time` reference: patching the shared `time` module would also freeze
    # pytest's own clock and sleeps.
    monkeypatch.setattr(
        download,
        "time",
        SimpleNamespace(
            monotonic=time.monotonic, time=time.time, sleep=lambda seconds: None
        ),
    )


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
    # This server sends no ETag, so drops cannot be resumed: the first attempt is progress, and the three
    # restarts that get no further are the stalls that exhaust `attempts`.
    session = _FlakySession(n_failures=10)
    cache_dir = tmp_path / "cache"

    with pytest.raises(ValueError, match="after 4 attempts"):
        download_zip(URL, tmp_path / "raw", session, cache_dir=cache_dir)

    assert len(session.calls) == 4
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


class _TrickleSession:
    """Serves the body a few bytes at a time while a fake clock advances, like a throttled PhysioNet."""

    def __init__(self, clock: list[float], seconds_per_chunk: float):
        self.clock = clock
        self.seconds_per_chunk = seconds_per_chunk
        self.calls = 0

    def get(self, url, stream=False, timeout=None, headers=None):
        self.calls += 1
        session = self

        class _Trickle(_Response):
            def iter_content(self, chunk_size):
                for i in range(0, len(self.body), 16):
                    session.clock[0] += session.seconds_per_chunk
                    yield self.body[i : i + 16]

        return _Trickle(_zip_bytes())


def test_trickling_download_fails_within_budget_without_retrying(
    tmp_path: Path, monkeypatch
):
    # Read timeouts never fire while bytes keep arriving, so only the wall-clock budget can stop this.
    clock = [0.0]
    monkeypatch.setattr(download.time, "monotonic", lambda: clock[0])
    session = _TrickleSession(clock, seconds_per_chunk=5.0)
    dest = tmp_path / "demo.zip"

    with pytest.raises(
        DownloadBudgetExceeded, match="exceeded its 60s budget on attempt 1 "
    ):
        _fetch_to_file(URL, dest, session, attempts=3, budget_s=60)

    assert session.calls == 1
    assert list(tmp_path.iterdir()) == []


def test_session_does_not_retry_read_timeouts():
    # download_zip restarts whole downloads itself; adapter-level read retries would multiply stalls.
    retry = make_session().get_adapter("https://physionet.org").max_retries
    assert retry.read == 0
    assert retry.connect == 3
    assert 503 in retry.status_forcelist
    assert not retry.respect_retry_after_header


class _RangeServer:
    """Serves `body` with an ETag, honouring `Range` + `If-Range`, and drops responses after set byte counts.

    `drop_after[i]` is how many bytes of the i-th response are sent before the connection breaks (None: all).
    """

    def __init__(self, body: bytes, drop_after: list[int | None], etag: str = '"v1"'):
        self.body = body
        self.drop_after = drop_after
        self.etag = etag
        self.calls: list[dict] = []
        self.bytes_served = 0

    def get(self, url, stream=False, timeout=None, headers=None):
        headers = headers or {}
        start = 0
        if "Range" in headers and headers.get("If-Range") == self.etag:
            start = int(headers["Range"].removeprefix("bytes=").rstrip("-"))
        drop = (
            self.drop_after[len(self.calls)]
            if len(self.calls) < len(self.drop_after)
            else None
        )
        self.calls.append({"headers": dict(headers), "start": start})
        server = self

        class _Ranged(_Response):
            def iter_content(self, chunk_size):
                payload = server.body[start:]
                sent = payload if drop is None else payload[:drop]
                for i in range(0, len(sent), 16):
                    server.bytes_served += len(sent[i : i + 16])
                    yield sent[i : i + 16]
                if drop is not None:
                    raise requests.exceptions.ChunkedEncodingError("Connection broken")

        response = _Ranged(self.body)
        response.headers = {"ETag": self.etag}
        if start:
            response.status_code = 206
            response.headers["Content-Range"] = (
                f"bytes {start}-{len(self.body) - 1}/{len(self.body)}"
            )
            response.headers["Content-Length"] = str(len(self.body) - start)
        else:
            response.headers["Content-Length"] = str(len(self.body))
        return response


def test_resumes_after_drops_without_redownloading(tmp_path: Path):
    body = _zip_bytes()
    # Four drops, each after some progress: more failures than `attempts`, but none of them is a stall.
    server = _RangeServer(body, drop_after=[100, 100, 100, 100])
    dest = tmp_path / "demo.zip"

    _fetch_to_file(URL, dest, server, attempts=3)

    assert dest.read_bytes() == body
    assert server.bytes_served == len(body), (
        "resumed attempts must not re-send earlier bytes"
    )
    assert [c["start"] for c in server.calls] == [0, 100, 200, 300, 400]
    assert server.calls[1]["headers"] == {"Range": "bytes=100-", "If-Range": '"v1"'}
    assert not dest.with_name("demo.zip.part").exists()


def test_changed_file_restarts_from_zero(tmp_path: Path):
    body = _zip_bytes()
    server = _RangeServer(body, drop_after=[100])
    dest = tmp_path / "demo.zip"
    real_get = server.get

    def get_after_change(*args, **kwargs):
        if server.calls:
            server.etag = '"v2"'  # the file changed between attempts, so If-Range no longer matches
        return real_get(*args, **kwargs)

    server.get = get_after_change
    _fetch_to_file(URL, dest, server, attempts=3)

    assert dest.read_bytes() == body
    assert [c["start"] for c in server.calls] == [0, 0]


def test_unexpected_content_range_is_not_spliced(tmp_path: Path):
    body = _zip_bytes()
    server = _RangeServer(body, drop_after=[100])
    dest = tmp_path / "demo.zip"
    real_get = server.get

    def misaligned_get(url, stream=False, timeout=None, headers=None):
        response = real_get(url, stream=stream, timeout=timeout, headers=headers)
        if response.status_code == 206 and len(server.calls) == 2:
            response.headers["Content-Range"] = f"bytes 50-{len(body) - 1}/{len(body)}"
        return response

    server.get = misaligned_get
    _fetch_to_file(URL, dest, server, attempts=3)

    assert dest.read_bytes() == body
    assert [c["start"] for c in server.calls] == [0, 100, 0]


def test_stalls_without_progress_give_up_after_attempts(tmp_path: Path):
    # Every response drops before sending anything, so no attempt makes progress.
    server = _RangeServer(_zip_bytes(), drop_after=[0] * 10)
    dest = tmp_path / "demo.zip"

    with pytest.raises(ValueError, match="after 3 attempts"):
        _fetch_to_file(URL, dest, server, attempts=3)

    assert len(server.calls) == 3
    assert list(tmp_path.iterdir()) == []
