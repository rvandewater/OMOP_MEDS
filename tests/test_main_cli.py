"""The `OMOP_MEDS` entry point's orchestration, with pre-MEDS and the MEDS-Transforms subprocess faked out.

`main` decides what is deleted, which event config MEDS-Extract reads, and the exact `MEDS_transform-pipeline`
command and environment. These tests pin that wiring without running either the pre-MEDS step or the pipeline.
"""

import logging
import os
import runpy
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import pytest
import yaml
from omegaconf import OmegaConf

import OMOP_MEDS.__main__ as cli
from OMOP_MEDS import ETL_CFG, EVENT_CFG, MAIN_CFG
from OMOP_MEDS import __version__ as PKG_VERSION
from OMOP_MEDS.commands import MEDS_PIPELINE_CMD, run_command

EVENT_TABLES = [t for t in OmegaConf.load(EVENT_CFG) if not str(t).startswith("_")]
BASE_PATH = os.pathsep.join(["/usr/bin", "/bin"])


@pytest.fixture
def fake_pipeline(monkeypatch):
    """Replace every side-effecting step of `main` with a recorder."""
    calls: dict = {"run_command": [], "finish": [], "renamed": [], "pre_meds": []}

    def fake_pre_meds(cfg):
        pre_meds_dir = Path(cfg.pre_MEDS_dir)
        # What existed when pre-MEDS started, i.e. after main's overwrite handling.
        calls["pre_meds"].append(
            {
                "root_existed": Path(cfg.root_output_dir).exists(),
                "pre_meds_existed": pre_meds_dir.exists(),
            }
        )
        pre_meds_dir.mkdir(parents=True, exist_ok=True)
        for table in calls["tables"]:
            (pre_meds_dir / f"{table}.parquet").touch()
        (pre_meds_dir / "codes.parquet").touch()

    def fake_run_command(command_parts, env=None, **kwargs):
        calls["run_command"].append({"command": list(command_parts), "env": env})

    monkeypatch.setattr(cli, "pre_MEDS_transform", fake_pre_meds)
    monkeypatch.setattr(cli, "run_command", fake_run_command)
    monkeypatch.setattr(
        cli, "finish_codes_metadata", lambda *args: calls["finish"].append(args)
    )
    monkeypatch.setattr(
        cli, "rename_demo_files", lambda d: calls["renamed"].append(Path(d))
    )

    monkeypatch.setenv("DATASET_NAME", "TEST_DATASET")
    monkeypatch.setenv("RAW_DATASET_VERSION", "0.9")
    monkeypatch.setenv("OMOP_VERSION", "5.4")
    monkeypatch.setenv("PATH", BASE_PATH)
    monkeypatch.delenv("N_WORKERS", raising=False)
    calls["tables"] = list(EVENT_TABLES)
    return calls


def _cfg(tmp_path: Path, **overrides):
    cfg = OmegaConf.load(MAIN_CFG)
    cfg.root_output_dir = str(tmp_path / "out")
    cfg.raw_input_dir = str(tmp_path / "raw")
    cfg.pre_MEDS_dir = str(tmp_path / "out" / "pre_MEDS")
    cfg.MEDS_cohort_dir = str(tmp_path / "out" / "MEDS_cohort")
    cfg.do_download = False
    cfg.do_demo = False
    cfg.do_overwrite = False
    for key, value in overrides.items():
        cfg[key] = value
    return cfg


def _run(cfg, calls) -> dict:
    cli.main.__wrapped__(cfg)
    (call,) = calls["run_command"]
    return call


def test_pipeline_command_and_environment(tmp_path, fake_pipeline):
    cfg = _cfg(tmp_path)

    call = _run(cfg, fake_pipeline)

    cohort_dir = (tmp_path / "out" / "MEDS_cohort").resolve()
    assert call["command"] == [
        MEDS_PIPELINE_CMD,
        str(ETL_CFG.resolve()),
        "--overrides",
        f"output_dir={cohort_dir}",
        "do_overwrite=False",
    ]
    env = call["env"]
    assert env["DATASET_NAME"] == "TEST_DATASET"
    assert env["DATASET_VERSION"] == f"0.9:{PKG_VERSION}:OMOP_5.4"
    assert env["PRE_MEDS_DIR"] == str((tmp_path / "out" / "pre_MEDS").resolve())
    assert env["MEDS_COHORT_DIR"] == str(cohort_dir)

    scripts_dir = sysconfig.get_path("scripts")
    if shutil.which(MEDS_PIPELINE_CMD, path=scripts_dir):
        assert env["PATH"] == os.pathsep.join([scripts_dir, BASE_PATH])
    else:  # pragma: no cover - only when console scripts live outside this interpreter's scripts dir
        assert env["PATH"] == BASE_PATH

    assert cohort_dir.is_dir()
    assert fake_pipeline["finish"] == [
        (Path(cfg.MEDS_cohort_dir), Path(cfg.pre_MEDS_dir), EVENT_CFG)
    ]
    assert fake_pipeline["renamed"] == []


def test_optional_runner_flags_and_overrides(tmp_path, fake_pipeline, monkeypatch):
    monkeypatch.setenv("N_WORKERS", "3")
    cfg = _cfg(
        tmp_path,
        stage_runner_fp="/configs/runner.yaml",
        seed=7,
        do_profile=True,
    )

    call = _run(cfg, fake_pipeline)

    cohort_dir = (tmp_path / "out" / "MEDS_cohort").resolve()
    assert call["command"] == [
        MEDS_PIPELINE_CMD,
        str(ETL_CFG.resolve()),
        "--stage_runner_fp=/configs/runner.yaml",
        "--do_profile",
        "--overrides",
        f"output_dir={cohort_dir}",
        "do_overwrite=False",
        "seed=7",
        "++parallelize.n_workers=3",
        "++parallelize.launcher=joblib",
    ]


@pytest.mark.parametrize("n_workers", [None, "1"], ids=["unset", "one"])
def test_single_worker_adds_no_parallelize_overrides(
    tmp_path, fake_pipeline, monkeypatch, n_workers
):
    if n_workers is not None:
        monkeypatch.setenv("N_WORKERS", n_workers)

    call = _run(_cfg(tmp_path), fake_pipeline)

    assert not [p for p in call["command"] if "parallelize" in p]


def test_unpruned_event_config_is_the_packaged_one(tmp_path, fake_pipeline):
    call = _run(_cfg(tmp_path), fake_pipeline)

    assert call["env"]["EVENT_CONVERSION_CONFIG_FP"] == str(Path(EVENT_CFG).resolve())
    assert not (tmp_path / "out" / "pre_MEDS" / "event_configs.yaml").exists()


def test_missing_pre_meds_table_is_pruned_from_event_config(
    tmp_path, fake_pipeline, caplog
):
    fake_pipeline["tables"] = [t for t in EVENT_TABLES if t != "note"]

    with caplog.at_level(logging.WARNING, logger=cli.logger.name):
        call = _run(_cfg(tmp_path), fake_pipeline)

    pruned_fp = tmp_path / "out" / "pre_MEDS" / "event_configs.yaml"
    assert call["env"]["EVENT_CONVERSION_CONFIG_FP"] == str(pruned_fp.resolve())
    pruned = yaml.safe_load(pruned_fp.read_text())
    assert pruned["_defaults"] == {"subject_id": "$person_id"}
    assert "note" not in pruned
    assert set(pruned) == {"_defaults", *fake_pipeline["tables"]}
    assert "Removing table note from event config." in caplog.text
    # The codes are linked to their concepts through the config MEDS-Extract used.
    ((_, _, event_cfg_path),) = fake_pipeline["finish"]
    assert event_cfg_path == pruned_fp


def test_athena_csv_dir_fills_the_written_codes_metadata(
    tmp_path, fake_pipeline, monkeypatch
):
    codes_fp = tmp_path / "out" / "MEDS_cohort" / "metadata" / "codes.parquet"
    enriched = []
    monkeypatch.setattr(cli, "finish_codes_metadata", lambda *args: codes_fp)
    monkeypatch.setattr(
        cli, "enrich_codes_metadata_with_athena", lambda *args: enriched.append(args)
    )
    cfg = _cfg(tmp_path, athena_csv_dir="~/athena")

    _run(cfg, fake_pipeline)

    assert enriched == [
        (codes_fp, Path(cfg.pre_MEDS_dir), Path("~/athena").expanduser())
    ]


@pytest.mark.parametrize(
    ("athena_csv_dir", "codes_written"),
    [(None, True), ("/athena", False)],
    ids=["no-athena-dir", "no-codes-metadata"],
)
def test_athena_fallback_is_skipped(
    tmp_path, fake_pipeline, monkeypatch, athena_csv_dir, codes_written
):
    codes_fp = tmp_path / "codes.parquet" if codes_written else None
    enriched = []
    monkeypatch.setattr(cli, "finish_codes_metadata", lambda *args: codes_fp)
    monkeypatch.setattr(
        cli, "enrich_codes_metadata_with_athena", lambda *args: enriched.append(args)
    )

    _run(_cfg(tmp_path, athena_csv_dir=athena_csv_dir), fake_pipeline)

    assert enriched == []


def test_overwrite_removes_outputs_including_pre_meds_outside_root(
    tmp_path, fake_pipeline
):
    outside_pre_meds = tmp_path / "elsewhere" / "pre_MEDS"
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "stale.txt").write_text("old")
    outside_pre_meds.mkdir(parents=True)
    (outside_pre_meds / "stale.parquet").write_text("old")
    cfg = _cfg(tmp_path, do_overwrite=True, pre_MEDS_dir=str(outside_pre_meds))

    call = _run(cfg, fake_pipeline)

    assert fake_pipeline["pre_meds"] == [
        {"root_existed": False, "pre_meds_existed": False}
    ]
    assert not (tmp_path / "out" / "stale.txt").exists()
    assert not (outside_pre_meds / "stale.parquet").exists()
    assert "do_overwrite=True" in call["command"]


def test_without_overwrite_existing_outputs_are_kept(tmp_path, fake_pipeline):
    (tmp_path / "out" / "pre_MEDS").mkdir(parents=True)
    (tmp_path / "out" / "keep.txt").write_text("keep")
    (tmp_path / "out" / "pre_MEDS" / "keep.parquet").write_text("keep")

    _run(_cfg(tmp_path), fake_pipeline)

    assert fake_pipeline["pre_meds"] == [
        {"root_existed": True, "pre_meds_existed": True}
    ]
    assert (tmp_path / "out" / "keep.txt").read_text() == "keep"
    assert (tmp_path / "out" / "pre_MEDS" / "keep.parquet").read_text() == "keep"


def test_demo_input_files_are_renamed_without_downloading(tmp_path, fake_pipeline):
    _run(_cfg(tmp_path, do_demo=True), fake_pipeline)

    assert fake_pipeline["renamed"] == [tmp_path / "raw"]


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_version_flag_prints_versions_and_exits(monkeypatch, capsys):
    import hydra

    monkeypatch.setattr(sys, "argv", ["OMOP_MEDS", "--version"])

    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("OMOP_MEDS.__main__", run_name="__main__")

    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert f"OMOP_MEDS version: {PKG_VERSION}" in out
    assert f"Hydra version: {hydra.__version__}" in out


# --- run_command ------------------------------------------------------------------------------------------


def _fake_runner(returncode=0, stdout=b"", stderr=b""):
    seen: dict = {}

    def runner(cmd, capture_output, env):
        seen["cmd"] = list(cmd)
        seen["env"] = env
        return subprocess.CompletedProcess(cmd, returncode, stdout, stderr)

    return runner, seen


@pytest.mark.parametrize(
    "cfg",
    [
        {"do_overwrite": True, "seed": 3, "do_profile": True},
        OmegaConf.create({"do_overwrite": True, "seed": 3, "do_profile": True}),
    ],
    ids=["dict", "DictConfig"],
)
def test_run_command_appends_cfg_overrides(cfg):
    runner, seen = _fake_runner()

    run_command(["tool", "arg"], cfg=cfg, runner_fn=runner)

    assert seen["cmd"] == ["tool", "arg", "do_overwrite=True", "seed=3", "--do_profile"]


def test_run_command_keeps_explicit_overrides_and_skips_unset_ones():
    runner, seen = _fake_runner()

    run_command(
        ["tool", "do_overwrite=False", "seed=1"],
        cfg={"do_overwrite": True, "seed": 3},
        runner_fn=runner,
    )
    assert seen["cmd"] == ["tool", "do_overwrite=False", "seed=1"]

    run_command(["tool"], cfg={}, runner_fn=runner)
    assert seen["cmd"] == ["tool"]


def test_run_command_logs_stderr_as_warning(caplog):
    runner, _ = _fake_runner(stdout=b"done", stderr=b"a deprecation notice")

    with caplog.at_level(logging.INFO, logger="OMOP_MEDS.commands"):
        run_command(["tool"], runner_fn=runner)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "a deprecation notice" in warnings[0].getMessage()
    assert any("done" in r.getMessage() for r in caplog.records)


def test_run_command_without_stderr_logs_no_warning(caplog):
    runner, _ = _fake_runner(stdout=b"done")

    with caplog.at_level(logging.INFO, logger="OMOP_MEDS.commands"):
        run_command(["tool"], runner_fn=runner)

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_run_command_raises_with_output_on_failure():
    runner, _ = _fake_runner(returncode=2, stdout=b"partial out", stderr=b"boom")

    with pytest.raises(ValueError, match="return code 2") as excinfo:
        run_command(["tool"], runner_fn=runner)

    assert "partial out" in str(excinfo.value)
    assert "boom" in str(excinfo.value)
