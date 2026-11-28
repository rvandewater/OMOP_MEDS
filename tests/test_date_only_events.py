"""Exports that leave the optional `*_datetime` columns empty and fill only the required `*_date` columns.

OMOP requires an event's date but not its datetime. An event timed only by `$..._datetime` would be dropped
when that column is empty, so every such time falls back to its date: at the end of the day (23:59:59), or, for
the starts of visits and observation periods, at its start (00:00).
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
#: Times at the start of their date when unknown: the starts of the intervals that anchor the other events.
START_OF_DAY = {
    ("visit_occurrence", "visit"),
    ("visit_detail", "visit_detail"),
    ("observation_period", "observation_period"),
}
#: Times resolved in pre-MEDS: birth, death and the note's last-edit-aware `preferred_time`.
PRE_RESOLVED = {
    ("person_birth_death", "age"),
    ("person_birth_death", "death_time"),
    ("note", "note"),
}


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
def test_every_unknown_time_falls_back_to_its_date(version):
    for table, event, time in _event_times():
        if (table, event) in PRE_RESOLVED:
            continue
        stem = re.search(r"\$(\w+?)_date", time)[1]
        if (table, event) in START_OF_DAY:
            fallback = f"${stem}_date"
        else:
            fallback = f"(${stem}_date @ 11:59:59 p.m.)"
        assert time in {fallback, f"${stem}_datetime ?? {fallback}"}, (table, event)
        assert f"{stem}_date" in CONFIGS[version][table]["output_data_cols"], (
            table,
            event,
        )


def _events(cohort: Path) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(fp) for fp in sorted((cohort / "data").rglob("*.parquet"))]
    )


def test_an_export_without_datetimes_times_every_event_by_its_date(
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
    anchor = pl.col("table_name").is_in([t for t, _ in START_OF_DAY]) & pl.col(
        "code"
    ).str.ends_with("//start")
    assert dates.filter(timed & anchor).height > 0
    assert (
        dates.filter(timed)
        .select(
            (
                pl.col("time").dt.time()
                == pl.when(anchor).then(pl.time(0)).otherwise(pl.time(23, 59, 59))
            ).all()
        )
        .item()
    )

    # The same events, each at the end (visit starts: the start) of its date. Events that differed only in their
    # time of day are now identical rows, which MEDS-Extract keeps once.
    day = pl.col("time").dt.truncate("1d")
    expected = (
        full.filter(timed)
        .with_columns(
            time=pl.when(anchor).then(day).otherwise(day.dt.offset_by("23h59m59s"))
        )
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
