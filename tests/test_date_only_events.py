"""Exports that leave the optional `*_datetime` columns empty and fill only the required `*_date` columns.

OMOP requires an event's date but not its datetime. An event timed only by `$..._datetime` would be dropped
when that column is empty, so every such time falls back to the end of the event's date (23:59:59).
"""

import re
from pathlib import Path

import polars as pl
import pytest
import yaml
from omop_run import run_omop
from test_omop_versions import CONFIGS, _stage_demo_as

from OMOP_MEDS.omop_versions import SUPPORTED_OMOP_VERSIONS

EVENT_CONFIGS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "OMOP_MEDS"
    / "configs"
    / "event_configs.yaml"
)
DATETIME_TIME = re.compile(r"\$(\w+)_datetime\b")


def _event_times() -> list[tuple[str, str, str]]:
    """(table, event, time expression) of every event with a time."""
    cfg = yaml.safe_load(EVENT_CONFIGS.read_text())
    return [
        (table, event, block["time"])
        for table, events in cfg.items()
        if table not in {"etl", "sources", "_defaults"}
        for event, block in events.items()
        if isinstance(block, dict) and block.get("time")
    ]


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_every_datetime_time_falls_back_to_its_date_at_the_end_of_the_day(version):
    checked = 0
    for table, event, time in _event_times():
        match = DATETIME_TIME.fullmatch(time.split(" ?? ")[0])
        if not match:
            continue
        stem = match[1]
        assert time == f"${stem}_datetime ?? (${stem}_date @ 11:59:59 p.m.)", (
            table,
            event,
        )
        assert f"{stem}_date" in CONFIGS[version][table]["output_data_cols"], (
            table,
            event,
        )
        checked += 1
    assert checked == 12


def _events(cohort: Path) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(fp) for fp in sorted((cohort / "data").rglob("*.parquet"))]
    )


def test_an_export_without_datetimes_keeps_every_event_at_the_end_of_its_date(
    tmp_path: Path,
):
    with_datetimes = _stage_demo_as("5.4", tmp_path / "raw")
    date_only = tmp_path / "raw_date_only"
    date_only.mkdir()
    for fp in with_datetimes.glob("*.csv"):
        df = pl.read_csv(fp, infer_schema=False)
        df.with_columns(
            pl.lit(None, pl.Utf8).alias(c)
            for c in df.columns
            if c.endswith("_datetime")
        ).write_csv(date_only / fp.name)

    for raw in (with_datetimes, date_only):
        run_omop(tmp_path / f"out_{raw.name}", raw, "skip_stages=[sumeds_enrich]")
    full = _events(tmp_path / "out_raw")
    dates = _events(tmp_path / "out_raw_date_only")

    # Births come from year/month/day_of_birth instead, at midnight; static rows have no time.
    timed = (pl.col("code") != "MEDS_BIRTH") & pl.col("time").is_not_null()
    assert dates.filter(timed).height > 0
    assert (
        dates.filter(timed)
        .select((pl.col("time").dt.time() == pl.time(23, 59, 59)).all())
        .item()
    )

    # The same events, each at the end of its date. Events that differed only in their time of day are now
    # identical rows, which MEDS-Extract keeps once.
    expected = (
        full.filter(timed)
        .with_columns(pl.col("time").dt.truncate("1d").dt.offset_by("23h59m59s"))
        .unique()
    )
    assert expected.height > 0
    assert (
        dates.filter(timed)
        .sort(dates.columns, nulls_last=True)
        .equals(expected.sort(dates.columns, nulls_last=True))
    )
    untimed = ~timed & (pl.col("code") != "MEDS_BIRTH")
    assert (
        dates.filter(untimed)
        .sort(dates.columns, nulls_last=True)
        .equals(full.filter(untimed).sort(dates.columns, nulls_last=True))
    )
    assert (
        dates.filter(pl.col("code") == "MEDS_BIRTH").height
        == full.filter(pl.col("code") == "MEDS_BIRTH").height
    )
    assert dates.filter(pl.col("code") == "MEDS_DEATH").height > 0
