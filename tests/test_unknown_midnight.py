"""Midnight-filled `*_datetime` values: the OMOP CDM fills an unknown time of day with 00:00:00 (#54).

Such a time is unknown, like an empty one: the event is timed at the end of its date (23:59:59), a visit start at
its start (00:00), so that it never leaks into predictions made earlier that day.
"""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from omop_run import run_omop
from test_omop_versions import _stage_demo_as

from OMOP_MEDS.pre_meds_utils import null_unknown_midnight

DAY = datetime(2020, 1, 1)
END_OF_DAY = pl.duration(hours=23, minutes=59, seconds=59)


def test_null_unknown_midnight_nulls_only_exact_midnights():
    df = pl.LazyFrame(
        {
            "observation_datetime": [
                DAY,
                DAY.replace(second=1),
                DAY.replace(hour=23, minute=59, second=59),
                None,
            ],
            # Columns outside the CDM schema stay text when read from CSV.
            "xtn_note_last_edit_datetime": [
                "2020-01-01 00:00:00",
                "2020-01-01 00:00:01",
                "2020-01-01",
                None,
            ],
            "observation_date": [DAY.date()] * 4,
            # Only `*_datetime` columns with a time of day are changed.
            "odd_datetime": [DAY.date()] * 4,
            "visit_start_time": [DAY] * 4,
        }
    )

    out = null_unknown_midnight(df).collect()

    assert out["observation_datetime"].to_list() == [
        None,
        DAY.replace(second=1),
        DAY.replace(hour=23, minute=59, second=59),
        None,
    ]
    # A date-only text value has no time: it is unknown, like a midnight.
    assert out["xtn_note_last_edit_datetime"].to_list() == [
        None,
        DAY.replace(second=1),
        None,
        None,
    ]
    for column in ("observation_date", "odd_datetime", "visit_start_time"):
        assert out[column].equals(df.collect()[column])


def _set(column: str, values: dict[int, str | None]) -> pl.Expr:
    """`column` with the rows at the given positions set to the given values."""
    expr = pl.col(column)
    for i, value in values.items():
        expr = (
            pl.when(pl.int_range(pl.len()) == i)
            .then(pl.lit(value, pl.Utf8))
            .otherwise(expr)
        )
    return expr.alias(column)


def _stage_with_midnights(raw: Path) -> Path:
    """The demo, with a null and a midnight visit start, a midnight visit end and notes at midnight."""
    _stage_demo_as("5.4", raw)
    visits = pl.read_csv(raw / "visit_occurrence.csv", infer_schema=False)
    visits.with_columns(
        _set(
            "visit_start_datetime",
            {0: None, 1: f"{visits['visit_start_date'][1]} 00:00:00"},
        ),
        _set("visit_end_datetime", {1: f"{visits['visit_end_date'][1]} 00:00:00"}),
    ).write_csv(raw / "visit_occurrence.csv")

    # Three notes at midnight: without a last edit, last edited the next morning, and last edited that day.
    notes = pl.read_csv(raw / "note.csv", infer_schema=False)
    assert notes["note_date"][:3].to_list() == [
        "2023-01-01",
        "2023-01-02",
        "2023-01-03",
    ]
    notes.with_columns(
        _set("note_datetime", {i: f"2023-01-0{i + 1} 00:00:00" for i in range(3)}),
        _set(
            "xtn_note_last_edit_datetime",
            {0: None, 1: "2023-01-03 08:00:00", 2: "2023-01-03 10:00:00"},
        ),
        _set("xtn_note_last_edit_date", {0: None, 1: "2023-01-03", 2: "2023-01-03"}),
    ).write_csv(raw / "note.csv")
    return raw


def _events(cohort: Path) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(fp) for fp in sorted((cohort / "data").rglob("*.parquet"))]
    )


@pytest.fixture(scope="module")
def runs(tmp_path_factory) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The raw visits and the events with and without `midnight_is_unknown_time`."""
    tmp = tmp_path_factory.mktemp("unknown_midnight")
    raw = _stage_with_midnights(tmp / "raw")
    run_omop(tmp / "on", raw, "skip_stages=[sumeds_enrich]")
    run_omop(
        tmp / "off",
        raw,
        "skip_stages=[sumeds_enrich]",
        "overrides=['++stage_cfg.midnight_is_unknown_time=false']",
    )
    visits = pl.read_csv(raw / "visit_occurrence.csv", try_parse_dates=True)
    return visits, _events(tmp / "on"), _events(tmp / "off")


IS_MIDNIGHT = pl.col("time").dt.time() == pl.time(0)
VISIT_START = (pl.col("table_name") == "visit_occurrence") & pl.col(
    "code"
).str.ends_with("//start")


def test_midnight_times_move_to_the_end_of_their_day(runs):
    _, on, off = runs
    assert off.filter((pl.col("table_name") == "observation") & IS_MIDNIGHT).height > 0
    assert off.filter((pl.col("code") == "MEDS_DEATH") & IS_MIDNIGHT).height > 0
    assert (
        off.filter(
            ~VISIT_START & IS_MIDNIGHT & pl.col("code").str.ends_with("//end")
        ).height
        > 0
    )

    # Every event is the same, but for its time: a midnight one is at 23:59:59, except births and visit starts.
    # Notes resolve their time from their last edit too; see below.
    keep_midnight = (pl.col("code") == "MEDS_BIRTH") | VISIT_START
    expected = off.filter(pl.col("table_name") != "note").with_columns(
        time=pl.when(IS_MIDNIGHT & ~keep_midnight)
        .then(pl.col("time") + END_OF_DAY)
        .otherwise(pl.col("time"))
    )
    actual = on.filter(pl.col("table_name") != "note")
    assert actual.sort(on.columns, nulls_last=True).equals(
        expected.sort(on.columns, nulls_last=True)
    )
    assert (
        on.filter(IS_MIDNIGHT & ~keep_midnight & pl.col("time").is_not_null()).height
        == 0
    )


def test_visit_starts_with_an_unknown_time_are_at_the_start_of_their_day(runs):
    visits, on, off = runs
    for events in (on, off):
        starts = events.filter(VISIT_START).join(
            visits.head(2).select(
                "visit_occurrence_id", "visit_start_date", "visit_end_date"
            ),
            on="visit_occurrence_id",
        )
        # The null start and the midnight start are both unknown: the start of their date.
        assert starts.height == 2
        assert (
            starts["time"] == starts["visit_start_date"].cast(pl.Datetime("us"))
        ).all()

    midnight_visit = visits["visit_occurrence_id"][1]
    end = pl.col("visit_occurrence_id") == midnight_visit
    end &= pl.col("code").str.ends_with("//end")
    end_date = datetime.combine(visits["visit_end_date"][1], datetime.min.time())
    assert on.filter(end)["time"].to_list() == [
        end_date.replace(hour=23, minute=59, second=59)
    ]
    assert off.filter(end)["time"].to_list() == [end_date]


def test_notes_at_midnight_are_at_the_end_of_their_day_unless_edited_later(runs):
    _, on, off = runs
    note_times = dict(
        on.filter(pl.col("table_name") == "note").select("link_id", "time").iter_rows()
    )
    assert note_times[1] == datetime(2023, 1, 1, 23, 59, 59)
    assert note_times[2] == datetime(2023, 1, 3, 8)  # the last edit is later
    assert note_times[3] == datetime(2023, 1, 3, 23, 59, 59)  # the last edit is not

    off_times = dict(
        off.filter(pl.col("table_name") == "note").select("link_id", "time").iter_rows()
    )
    assert off_times[1] == datetime(2023, 1, 1)
    assert off_times[3] == datetime(2023, 1, 3, 10)


def test_births_are_unchanged(runs):
    _, on, off = runs
    births = [
        e.filter(pl.col("code") == "MEDS_BIRTH").sort("subject_id") for e in (on, off)
    ]
    assert births[0].height > 0
    assert births[0].equals(births[1])
