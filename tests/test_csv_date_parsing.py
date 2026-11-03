import datetime
import gzip
from pathlib import Path

import polars as pl
import pytest
from omop_schema.utils import get_schema_loader

from OMOP_MEDS.pre_meds_data_loader import load_raw_file

HEADER = "condition_era_id,person_id,condition_concept_id,condition_era_start_date,condition_era_end_date,condition_occurrence_count\n"


def _rows(start_dates: list[str]) -> str:
    return "".join(
        f"{i},1,320128,{d},2005-06-01,1\n" for i, d in enumerate(start_dates, start=1)
    )


def _write(fp: Path, start_dates: list[str]) -> Path:
    body = HEADER + _rows(start_dates)
    if fp.suffix == ".gz":
        fp.write_bytes(gzip.compress(body.encode()))
    else:
        fp.write_text(body)
    return fp


def _start_dates(fp: Path) -> list[datetime.date | None]:
    df = load_raw_file(fp, get_schema_loader(5.4)).collect()
    assert df.schema["condition_era_start_date"] == pl.Date
    return df["condition_era_start_date"].to_list()


# Exported by SQL Server / Postgres CSV dumps (e.g. the OHDSI Synthea27Nj OMOP 5.4 dataset).
MIDNIGHT_FORMS = [
    "2005-05-07 00:00:00",
    "2005-05-07T00:00:00",
    "2005-05-07 00:00:00.000",
]


@pytest.mark.parametrize("value", MIDNIGHT_FORMS)
@pytest.mark.parametrize("suffix", [".csv", ".csv.gz"])
def test_midnight_timestamps_in_date_columns_parse_as_dates(
    tmp_path: Path, value: str, suffix: str
):
    fp = _write(tmp_path / f"condition_era{suffix}", ["2005-05-06", value, ""])
    assert _start_dates(fp) == [
        datetime.date(2005, 5, 6),
        datetime.date(2005, 5, 7),
        None,
    ]


def test_sharded_csv_directory(tmp_path: Path):
    table_dir = tmp_path / "condition_era"
    table_dir.mkdir()
    _write(table_dir / "000.csv", ["2005-05-07 00:00:00"])
    _write(table_dir / "001.csv", ["2005-05-08"])
    assert sorted(_start_dates(table_dir)) == [
        datetime.date(2005, 5, 7),
        datetime.date(2005, 5, 8),
    ]


@pytest.mark.parametrize("value", ["2005/05/07", "07-05-2005"])
def test_previously_accepted_formats_are_unchanged(tmp_path: Path, value: str):
    fp = _write(tmp_path / "condition_era.csv", [value])
    assert _start_dates(fp) == [datetime.date(2005, 5, 7)]


@pytest.mark.parametrize("value", ["2005-05-07 13:45:00", "garbage"])
def test_non_midnight_times_and_garbage_still_fail(tmp_path: Path, value: str):
    # Only a midnight time is dropped; anything else would lose information, so it must not become null.
    fp = _write(tmp_path / "condition_era.csv", [value])
    with pytest.raises(
        (pl.exceptions.ComputeError, pl.exceptions.InvalidOperationError)
    ):
        _start_dates(fp)
