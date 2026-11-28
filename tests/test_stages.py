"""The OMOP extension stages of the `OMOP` spec, and the deprecated `OMOP_MEDS` command.

`omop_pre_meds` must write every table the spec reads, so that the one registered spec fits datasets without
some OMOP tables; `omop_codes_metadata` and `sumeds_enrich` run after MEDS-Extract (see
tests/test_run_with_enrichment.py).
"""

import json
import os
import shutil
import subprocess
from importlib.metadata import version
from pathlib import Path

import polars as pl
import pytest
from e2e_demo_test import _stage_local_demo_omop
from meds import DatasetMetadataSchema
from MEDS_extract.config import MessyConfig
from omegaconf import OmegaConf
from omop_run import SCRIPTS_DIR, run_omop

import OMOP_MEDS.pre_meds
from OMOP_MEDS import __version__
from OMOP_MEDS.pre_meds import main as pre_meds_main
from OMOP_MEDS.stages import (
    PRE_MEDS_DEFAULTS,
    PRE_MEDS_STAGE,
    SETTINGS_FILE,
    _pre_meds,
    pre_meds_config,
    record_provenance,
)

SPEC_TABLES = sorted(MessyConfig.load("OMOP").needed_source_columns())


@pytest.fixture(scope="module")
def raw(tmp_path_factory) -> Path:
    return _stage_local_demo_omop(tmp_path_factory.mktemp("raw") / "raw_input")


@pytest.fixture(scope="module")
def plain_cohort(tmp_path_factory, raw: Path) -> Path:
    cohort = tmp_path_factory.mktemp("plain") / "MEDS_cohort"
    run_omop(cohort, raw, "skip_stages=[sumeds_enrich]")
    return cohort


def _events(cohort: Path) -> pl.DataFrame:
    df = pl.concat(
        [pl.read_parquet(fp) for fp in sorted((cohort / "data").rglob("*.parquet"))]
    )
    return df.sort(df.columns, nulls_last=True)


def _stage_cfg(tmp_path: Path, worker: int = 0, **options):
    return OmegaConf.create(
        {
            "worker": worker,
            "input_dir": str(tmp_path / "raw"),
            "output_dir": str(tmp_path / "out"),
            "do_overwrite": False,
            "MESSY_config_fp": "pkg://OMOP_MEDS.configs.event_configs.yaml",
            "stage_cfg": {
                **PRE_MEDS_DEFAULTS,
                **options,
                "output_dir": str(tmp_path / "out" / PRE_MEDS_STAGE),
            },
        }
    )


def test_pre_stage_passes_its_options_and_the_spec_tables_to_pre_meds(
    tmp_path: Path, monkeypatch
):
    seen = []
    monkeypatch.setattr(OMOP_MEDS.pre_meds, "main", seen.append)

    _pre_meds(
        _stage_cfg(tmp_path, omop_version="5.5", prefer_source=True, limit_subjects=3)
    )

    (cfg,) = seen
    assert cfg.raw_input_dir == str(tmp_path / "raw")
    assert cfg.pre_MEDS_dir == str(tmp_path / "out" / PRE_MEDS_STAGE)
    assert (cfg.omop_version, cfg.prefer_source, cfg.limit_subjects) == ("5.5", True, 3)
    assert list(cfg.required_tables) == SPEC_TABLES
    assert {"person_birth_death", "note", "measurement"} <= set(SPEC_TABLES)


def test_pre_stage_works_only_in_worker_0(tmp_path: Path, monkeypatch):
    seen = []
    monkeypatch.setattr(OMOP_MEDS.pre_meds, "main", seen.append)

    _pre_meds(_stage_cfg(tmp_path, worker=1))

    assert seen == []


def test_tables_without_input_rows_are_written_empty_with_their_full_schema(
    raw: Path, tmp_path: Path
):
    full = tmp_path / "full"
    pre_meds_main(pre_meds_config(raw, full, required_tables=SPEC_TABLES))
    partial_raw = tmp_path / "raw"
    shutil.copytree(raw, partial_raw)
    (partial_raw / "note.csv").unlink()
    pl.read_csv(partial_raw / "observation.csv", infer_schema=False).head(0).write_csv(
        partial_raw / "observation.csv"
    )

    partial = tmp_path / "partial"
    pre_meds_main(pre_meds_config(partial_raw, partial, required_tables=SPEC_TABLES))

    needed = MessyConfig.load("OMOP").needed_source_columns()
    for table in ("note", "observation"):
        written = pl.read_parquet(partial / f"{table}.parquet")
        expected = pl.read_parquet(full / f"{table}.parquet")
        assert written.height == 0 < expected.height, table
        assert set(needed[table]) <= set(written.columns), table
        # Every column but the input's own extension columns, outside the CDM (the notes' last-edit times).
        missing = set(expected.columns) - set(written.columns)
        assert all(c.startswith("xtn_") for c in missing), (table, missing)
        assert all(
            written.schema[c] == dtype
            for c, dtype in expected.schema.items()
            if c not in missing
        )
    for table in SPEC_TABLES:
        assert (partial / f"{table}.parquet").exists() or (partial / table).is_dir(), (
            table
        )


def test_demo_download_layout_and_missing_tables_convert(
    raw: Path, tmp_path: Path, plain_cohort: Path
):
    """The MIMIC-IV OMOP demo zip nests its tables in `1_omop_data_csv` beside Achilles JSON files and prefixes
    its vocabulary tables with `2b_`; a dataset without notes gives the same events, less the notes."""
    nested = tmp_path / "download" / "mimic-iv-demo-omop-0.9"
    tables = nested / "1_omop_data_csv"
    shutil.copytree(raw, tables)
    (nested / "2_achilles_json").mkdir()
    (nested / "2_achilles_json" / "person.json").write_text("{}")
    for name in ("concept.csv", "concept_relationship.csv"):
        (tables / name).rename(tables / f"2b_{name}")
    (tables / "note.csv").unlink()

    cohort = tmp_path / "MEDS_cohort"
    run_omop(cohort, tmp_path / "download", "skip_stages=[sumeds_enrich]")

    events = _events(cohort)
    expected = _events(plain_cohort).filter(pl.col("table_name") != "note")
    assert events.equals(expected.select(events.columns))
    # The raw data is read as it is, never renamed.
    assert (tables / "2b_concept.csv").exists() and not (
        tables / "concept.csv"
    ).exists()
    codes = pl.read_parquet(cohort / "metadata" / "codes.parquet")
    assert codes["description"].is_not_null().sum() > 0


def test_deprecated_runner_command_runs_the_spec(raw: Path, tmp_path: Path):
    root = tmp_path / "root"
    command = [
        str(SCRIPTS_DIR / "OMOP_MEDS"),
        f"raw_input_dir={raw}",
        f"root_output_dir={root}",
        "do_enrich=False",
        "++limit_subjects=10",
    ]
    env = {
        **os.environ,
        "PATH": os.pathsep.join([str(SCRIPTS_DIR), os.environ.get("PATH", "")]),
        "DATASET_NAME": "OLD_RUNNER_NAME",
    }
    result = subprocess.run(command, capture_output=True, text=True, env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OMOP_MEDS is deprecated" in result.stderr
    assert "meds-extract-run spec=OMOP" in result.stderr
    cohort = root / "MEDS_cohort"
    assert _events(cohort)["subject_id"].n_unique() == 10
    dataset = json.loads((cohort / "metadata" / "dataset.json").read_text())
    assert dataset["dataset_name"] == "OLD_RUNNER_NAME"
    assert "sumeds_enrichment" not in dataset
    assert dataset["etl_name"] == "OMOP_MEDS"
    assert dataset["omop_meds"]["limit_subjects"] == 10


def test_deprecated_runner_command_rejects_removed_options(tmp_path: Path):
    result = subprocess.run(
        [
            str(SCRIPTS_DIR / "OMOP_MEDS"),
            f"root_output_dir={tmp_path}",
            "sumeds_args=[--strict]",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "no longer takes sumeds_args=" in result.stderr
    assert not (tmp_path / "MEDS_cohort").exists()


def test_deprecated_runner_argument_translation():
    """The examples of `OMOP_MEDS.__main__.translate` (pytest does not collect doctests from `__main__.py`)."""
    import doctest

    import OMOP_MEDS.__main__ as deprecated

    result = doctest.testmod(
        deprecated, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    assert result.attempted >= 3
    assert result.failed == 0


def test_pre_stage_writes_its_settings_without_paths_or_batching(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(OMOP_MEDS.pre_meds, "main", lambda cfg: None)
    monkeypatch.setenv("OMOP_VERSION", "5.4")

    _pre_meds(_stage_cfg(tmp_path, prefer_source=True))

    settings = json.loads(
        (tmp_path / "out" / PRE_MEDS_STAGE / SETTINGS_FILE).read_text()
    )
    assert settings["omop_meds_version"] == __version__
    assert (settings["omop_version"], settings["prefer_source"]) == ("5.4", True)
    assert settings["tables_to_ignore"] == PRE_MEDS_DEFAULTS["tables_to_ignore"]
    assert not any(k.startswith("pre_meds_") for k in settings)
    assert str(tmp_path) not in json.dumps(settings)


def _dataset_json(cohort: Path) -> Path:
    fp = cohort / "metadata" / "dataset.json"
    fp.parent.mkdir(parents=True)
    fp.write_text(
        json.dumps(
            {
                "dataset_name": "D",
                "dataset_version": "1",
                "etl_name": "MEDS_transforms",
                "etl_version": "0.7.3",
                "meds_version": "0.4.1",
                "created_at": "2026-01-01T00:00:00+00:00",
                "sumeds_enrichment": {"rules": "OMOP"},
            }
        )
    )
    return fp


def test_provenance_is_recorded_keeping_the_other_keys_idempotently(tmp_path: Path):
    fp = _dataset_json(tmp_path / "cohort")
    pre_MEDS_dir = tmp_path / PRE_MEDS_STAGE
    pre_MEDS_dir.mkdir()
    (pre_MEDS_dir / SETTINGS_FILE).write_text(
        json.dumps({"omop_meds_version": "0.0.1", "omop_version": "5.5"})
    )

    record_provenance(tmp_path / "cohort", pre_MEDS_dir, athena_fallback=True)
    first = fp.read_text()
    record_provenance(tmp_path / "cohort", pre_MEDS_dir, athena_fallback=True)

    assert fp.read_text() == first
    dataset = json.loads(first)
    DatasetMetadataSchema.validate(dataset)
    assert (dataset["dataset_name"], dataset["created_at"]) == (
        "D",
        "2026-01-01T00:00:00+00:00",
    )
    assert dataset["sumeds_enrichment"] == {"rules": "OMOP"}
    assert (dataset["etl_name"], dataset["etl_version"]) == ("OMOP_MEDS", __version__)
    assert dataset["omop_meds"] == {
        "omop_meds_version": "0.0.1",
        "omop_version": "5.5",
        "meds_extract_version": version("MEDS_extract"),
        "meds_transforms_version": version("MEDS_transforms"),
        "athena_fallback": True,
    }
    assert sorted(p.name for p in fp.parent.iterdir()) == ["dataset.json"]


def test_provenance_without_settings_records_the_versions(tmp_path: Path, caplog):
    fp = _dataset_json(tmp_path)

    record_provenance(tmp_path, tmp_path / PRE_MEDS_STAGE)

    assert SETTINGS_FILE in caplog.text
    assert set(json.loads(fp.read_text())["omop_meds"]) == {
        "omop_meds_version",
        "meds_extract_version",
        "meds_transforms_version",
        "athena_fallback",
    }


def test_provenance_without_dataset_json_warns_and_skips(tmp_path: Path, caplog):
    record_provenance(tmp_path, tmp_path / PRE_MEDS_STAGE)

    assert "dataset.json" in caplog.text
    assert not (tmp_path / "metadata").exists()
