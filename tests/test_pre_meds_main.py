"""Branches of the pre-MEDS step (`OMOP_MEDS.pre_meds.main`) beyond the happy path of the demo smoke test.

Every test runs on a private copy of `tests/demo_resources` (OMOP 5.3) under `tmp_path`.
"""

import logging
import shutil
from pathlib import Path

import polars as pl
import pytest
from omegaconf import OmegaConf
from omop_schema.utils import get_schema_loader

from OMOP_MEDS import MAIN_CFG, premeds_cfg
from OMOP_MEDS.__main__ import main as run_omop_meds
from OMOP_MEDS.pre_meds import main as pre_meds_main
from OMOP_MEDS.pre_meds_data_loader import load_raw_file

DEMO_RESOURCES_DIR = Path(__file__).resolve().parent / "demo_resources"
SCHEMA_5_3 = get_schema_loader(5.3)
N_DEMO_PERSONS = 100


def _stage_demo(raw: Path) -> Path:
    raw.mkdir(parents=True)
    for fp in DEMO_RESOURCES_DIR.glob("*.csv"):
        shutil.copy2(fp, raw / fp.name)
    concept = pl.read_csv(raw / "concept.csv")
    concept.select(
        pl.col("concept_id").cast(pl.Int64).alias("concept_id_1"),
        pl.col("concept_id").cast(pl.Int64).alias("concept_id_2"),
    ).with_columns(
        relationship_id=pl.lit("Maps to"),
        valid_start_date=pl.lit("1970-01-01"),
        valid_end_date=pl.lit("2099-12-31"),
        invalid_reason=pl.lit(None, dtype=pl.Utf8),
    ).write_csv(raw / "concept_relationship.csv")
    return raw


def _shard_as_parquet(raw: Path, table: str, n_shards: int) -> pl.DataFrame:
    """Replace `<table>.csv` by `n_shards` OMOP-typed parquet shards in `<table>/`; return the typed rows."""
    rows = load_raw_file(raw / f"{table}.csv", SCHEMA_5_3).collect()
    (raw / f"{table}.csv").unlink()
    (raw / table).mkdir()
    for i in range(n_shards):
        rows.gather_every(n_shards, offset=i).write_parquet(
            raw / table / f"part_{i}.parquet"
        )
    return rows


def _cfg(raw: Path, root: Path, **overrides):
    cfg = OmegaConf.load(MAIN_CFG)
    cfg.raw_input_dir = str(raw)
    cfg.root_output_dir = str(root)
    cfg.pre_MEDS_dir = str(root / "pre_MEDS")
    cfg.MEDS_cohort_dir = str(root / "MEDS_cohort")
    cfg.do_overwrite = True
    for key, value in overrides.items():
        OmegaConf.update(cfg, key, value, force_add=True)
    return cfg


BATCH_EVERY_SHARD = {
    "pre_meds_batching_row_threshold": 0,
    "pre_meds_batch_mode": "per_shard",
    "pre_meds_batch_input_rows": 0,
}


def _sorted(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort(df.columns, nulls_last=True)


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    return _stage_demo(tmp_path / "raw")


# --- shared package config ---------------------------------------------------------------------------------


def test_repeated_runs_keep_note_preferred_time_and_package_config(
    raw: Path, tmp_path: Path
):
    # main() used to pop `datetime_resolver` from the shared package config, so a second run in the same
    # process built notes without `preferred_time` (the leakage-safe last-edit timestamp).
    before = OmegaConf.to_container(premeds_cfg)
    notes = []
    for run in ("first", "second"):
        pre_meds_main(_cfg(raw, tmp_path / run))
        notes.append(pl.read_parquet(tmp_path / run / "pre_MEDS" / "note.parquet"))

    assert OmegaConf.to_container(premeds_cfg) == before
    assert all("preferred_time" in n.columns for n in notes)
    assert _sorted(notes[0]).equals(_sorted(notes[1]))  # row order is not deterministic


# --- batched loading of large tables -----------------------------------------------------------------------


def test_batched_table_is_written_as_a_subsharded_directory(raw: Path, tmp_path: Path):
    unbatched_raw = _stage_demo(tmp_path / "raw_unbatched")
    pre_meds_main(_cfg(unbatched_raw, tmp_path / "unbatched"))
    _shard_as_parquet(raw, "observation", n_shards=3)

    pre_meds_main(
        _cfg(
            raw,
            tmp_path / "batched",
            pre_meds_chunked_tables=["observation"],
            **BATCH_EVERY_SHARD,
        )
    )

    pre = tmp_path / "batched" / "pre_MEDS"
    parts = sorted(p.name for p in (pre / "observation").iterdir())
    assert parts == ["part_00001.parquet", "part_00002.parquet", "part_00003.parquet"]
    # Neither the pre-0.7 `observation.parquet/` layout nor the temporary parts directory is left behind.
    assert not (pre / "observation.parquet").exists()
    assert not (pre / ".observation_parts").exists()
    batched = pl.read_parquet(pre / "observation" / "*.parquet")
    unbatched = pl.read_parquet(tmp_path / "unbatched/pre_MEDS/observation.parquet")
    assert _sorted(batched).equals(_sorted(unbatched.select(batched.columns)))


def test_batched_table_with_a_single_part_is_a_plain_file(raw: Path, tmp_path: Path):
    _shard_as_parquet(raw, "observation", n_shards=1)

    pre_meds_main(
        _cfg(
            raw, tmp_path, pre_meds_chunked_tables=["observation"], **BATCH_EVERY_SHARD
        )
    )

    out = tmp_path / "pre_MEDS" / "observation.parquet"
    assert out.is_file()
    assert not (tmp_path / "pre_MEDS" / "observation").exists()
    assert pl.read_parquet(out).height > 0


def test_batched_table_without_known_subjects_is_skipped(
    raw: Path, tmp_path: Path, caplog
):
    rows = _shard_as_parquet(raw, "observation", n_shards=2)
    for part in (raw / "observation").iterdir():
        pl.read_parquet(part).with_columns(
            person_id=pl.lit(-1, pl.Int64)
        ).write_parquet(part)
    assert rows.height > 0

    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(
            _cfg(
                raw,
                tmp_path,
                pre_meds_chunked_tables=["observation"],
                **BATCH_EVERY_SHARD,
            )
        )

    pre = tmp_path / "pre_MEDS"
    assert not (pre / "observation").exists()
    assert not (pre / "observation.parquet").exists()
    assert not (pre / ".observation_parts").exists()
    assert "all processed batches were empty" in caplog.text


@pytest.mark.parametrize("with_care_site", [False, True])
def test_batched_visits_get_care_site_names(
    raw: Path, tmp_path: Path, with_care_site: bool
):
    # Two batches: the care_site lookup (or its absence) is resolved once and reused for the second batch.
    visits = pl.read_csv(raw / "visit_occurrence.csv", infer_schema=False)
    visits.with_columns(
        care_site_id=(pl.int_range(pl.len()) % 2 + 1).cast(pl.String)
    ).write_csv(raw / "visit_occurrence.csv")
    if with_care_site:
        pl.DataFrame(
            {"care_site_id": [1, 2], "care_site_name": ["ICU", "Ward"]}
        ).write_csv(raw / "care_site.csv")
    _shard_as_parquet(raw, "visit_occurrence", n_shards=2)

    pre_meds_main(
        _cfg(
            raw,
            tmp_path,
            pre_meds_chunked_tables=["visit_occurrence"],
            **BATCH_EVERY_SHARD,
        )
    )

    out = pl.read_parquet(tmp_path / "pre_MEDS" / "visit_occurrence" / "*.parquet")
    expected = {"ICU": 426, "Ward": 426} if with_care_site else {"1": 426, "2": 426}
    counts = out.group_by(pl.col("care_site_name").cast(pl.String)).len()
    assert dict(counts.iter_rows()) == expected


def test_unreadable_care_site_falls_back_to_care_site_id(
    raw: Path, tmp_path: Path, caplog
):
    visits = pl.read_csv(raw / "visit_occurrence.csv", infer_schema=False)
    visits.with_columns(care_site_id=pl.lit("7")).write_csv(
        raw / "visit_occurrence.csv"
    )
    (raw / "care_site").mkdir()
    (raw / "care_site" / "README.txt").write_text("no data files")

    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path))

    out = pl.read_parquet(tmp_path / "pre_MEDS" / "visit_occurrence.parquet")
    assert out["care_site_name"].cast(pl.String).unique().to_list() == ["7"]
    assert "Could not read care_site table" in caplog.text


def test_stale_parts_from_an_interrupted_batched_run_are_discarded(
    raw: Path, tmp_path: Path
):
    _shard_as_parquet(raw, "observation", n_shards=2)
    stale = tmp_path / "pre_MEDS" / ".observation_parts"
    stale.mkdir(parents=True)
    pl.DataFrame({"person_id": [-1]}).write_parquet(stale / "part_00009.parquet")

    pre_meds_main(
        _cfg(
            raw,
            tmp_path,
            do_overwrite=False,
            pre_meds_chunked_tables=["observation"],
            **BATCH_EVERY_SHARD,
        )
    )

    pre = tmp_path / "pre_MEDS"
    assert sorted(p.name for p in (pre / "observation").iterdir()) == [
        "part_00001.parquet",
        "part_00002.parquet",
    ]
    assert not stale.exists()


# --- resuming and skipping ---------------------------------------------------------------------------------


def test_completed_run_is_not_redone_without_overwrite(
    raw: Path, tmp_path: Path, caplog
):
    pre_meds_main(_cfg(raw, tmp_path))
    pre = tmp_path / "pre_MEDS"
    sentinel = pre / "person.parquet"
    sentinel.write_bytes(b"left alone")

    with caplog.at_level(logging.INFO, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path, do_overwrite=False))

    assert sentinel.read_bytes() == b"left alone"
    assert "already complete" in caplog.text


def test_partial_run_resumes_only_missing_tables(raw: Path, tmp_path: Path, caplog):
    pre_meds_main(_cfg(raw, tmp_path))
    pre = tmp_path / "pre_MEDS"
    (pre / ".done").unlink()
    kept = pre / "observation.parquet"
    kept.write_bytes(b"kept")  # a finished table must not be redone
    (pre / "note.parquet").unlink()

    with caplog.at_level(logging.INFO, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path, do_overwrite=False))

    assert "Partial run found" in caplog.text
    assert "Done with observation" in caplog.text
    assert kept.read_bytes() == b"kept"
    assert "preferred_time" in pl.read_parquet(pre / "note.parquet").columns
    assert (pre / ".done").is_file()


def test_legacy_batched_output_is_renamed_before_resuming(raw: Path, tmp_path: Path):
    pre_meds_main(_cfg(raw, tmp_path))
    pre = tmp_path / "pre_MEDS"
    # Simulate an older version's batched output: `observation.parquet/` holding parts.
    legacy = pre / "observation.parquet"
    part = pre / "part_00001.parquet"
    legacy.replace(part)
    legacy.mkdir()
    part.replace(legacy / "part_00001.parquet")

    pre_meds_main(_cfg(raw, tmp_path, do_overwrite=False))

    assert not legacy.exists()
    assert (pre / "observation" / "part_00001.parquet").is_file()


# --- per-table edge cases ----------------------------------------------------------------------------------


def test_prefer_source_is_flagged(raw: Path, tmp_path: Path, caplog):
    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path, prefer_source=True))

    assert "Preferring source values over mapped values" in caplog.text


def test_limit_subjects_restricts_every_table(raw: Path, tmp_path: Path):
    pre_meds_main(_cfg(raw, tmp_path, limit_subjects=5))

    pre = tmp_path / "pre_MEDS"
    persons = set(pl.read_parquet(pre / "person.parquet")["person_id"])
    assert len(persons) == 5
    for table in ("observation", "visit_occurrence", "note"):
        out = pre / f"{table}.parquet"
        if out.exists():
            assert set(pl.read_parquet(out)["person_id"]) <= persons, table


def test_unhandled_table_stays_visible_in_logs(raw: Path, tmp_path: Path, caplog):
    pl.DataFrame({"domain_id": ["Visit"], "domain_name": ["Visit"]}).write_csv(
        raw / "domain.csv"
    )

    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path))

    assert "No function needed for domain" in caplog.text
    assert not (tmp_path / "pre_MEDS" / "domain.parquet").exists()


def test_tables_on_the_module_ignore_list_are_not_read(
    raw: Path, tmp_path: Path, monkeypatch
):
    monkeypatch.setattr("OMOP_MEDS.pre_meds.IGNORE_TABLES", ["observation"])

    pre_meds_main(_cfg(raw, tmp_path))

    assert not (tmp_path / "pre_MEDS" / "observation.parquet").exists()
    assert (tmp_path / "pre_MEDS" / "visit_occurrence.parquet").exists()


def test_unsupported_omop_version_fails_before_writing_anything(
    raw: Path, tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("OMOP_VERSION", "6.0")

    with pytest.raises(ValueError, match="Unsupported OMOP CDM version '6.0'"):
        pre_meds_main(_cfg(raw, tmp_path))
    assert not (tmp_path / "pre_MEDS").exists()


def test_table_directory_without_data_files_is_skipped(
    raw: Path, tmp_path: Path, caplog
):
    (raw / "procedure_occurrence").mkdir()
    (raw / "procedure_occurrence" / "README.txt").write_text("not a table")

    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path))

    assert "Skipping procedure_occurrence because no readable files" in caplog.text
    assert not (tmp_path / "pre_MEDS" / "procedure_occurrence.parquet").exists()


def test_table_without_known_subjects_is_skipped(raw: Path, tmp_path: Path, caplog):
    obs = pl.read_csv(raw / "observation.csv", infer_schema=False)
    obs.with_columns(person_id=pl.lit("-1")).write_csv(raw / "observation.csv")

    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.pre_meds"):
        pre_meds_main(_cfg(raw, tmp_path))

    assert "Skipping observation as it is empty after preprocessing" in caplog.text
    assert not (tmp_path / "pre_MEDS" / "observation.parquet").exists()


# --- through MEDS-Extract ----------------------------------------------------------------------------------


def _meds_events(root: Path) -> pl.DataFrame:
    return pl.concat(
        [
            pl.read_parquet(f)
            for f in sorted((root / "MEDS_cohort/data").rglob("*.parquet"))
        ]
    )


def test_batched_pre_meds_output_converts_like_unbatched(tmp_path: Path):
    # MEDS-Extract >= 0.7 only reads `<table>/*.parquet` or `<table>.parquet`; the pre-0.7 batched layout
    # (`<table>.parquet/`) made the whole run fail with "No source files found for prefix 'observation'".
    unbatched_raw = _stage_demo(tmp_path / "raw_unbatched")
    batched_raw = _stage_demo(tmp_path / "raw_batched")
    _shard_as_parquet(batched_raw, "observation", n_shards=3)

    run_omop_meds.__wrapped__(_cfg(unbatched_raw, tmp_path / "unbatched"))
    run_omop_meds.__wrapped__(
        _cfg(
            batched_raw,
            tmp_path / "batched",
            pre_meds_chunked_tables=["observation"],
            **BATCH_EVERY_SHARD,
        )
    )

    assert (tmp_path / "batched/pre_MEDS/observation").is_dir()
    unbatched = _meds_events(tmp_path / "unbatched")
    batched = _meds_events(tmp_path / "batched")
    assert batched.height == unbatched.height > 0
    assert _sorted(batched).equals(_sorted(unbatched.select(batched.columns)))
