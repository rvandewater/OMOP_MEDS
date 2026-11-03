"""Guards for the MEDS-Extract (>= 0.8) / MEDS-Transforms (>= 0.7) configs and glue this package ships.

MEDS-Extract 0.7 reworked the pipeline and MESSY config formats; mistakes there either fail late inside a
stage subprocess or, worse, silently drop events. These tests check the shipped configs against the installed
MEDS-Extract itself, so a future upgrade that changes its canonical pipeline or parsing rules fails here first.
"""

import os
import re
import shutil
import subprocess
import sysconfig
from importlib.metadata import entry_points
from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest
import yaml
from MEDS_extract.config import EventConfig, MessyConfig
from omegaconf import OmegaConf

from OMOP_MEDS import ETL_CFG, EVENT_CFG
from OMOP_MEDS.event_config import prune_event_config
from OMOP_MEDS.commands import (
    MEDS_PIPELINE_CMD,
    path_with_scripts_dir_first,
    run_command,
)


def _stage_names(stages: list) -> list[str]:
    return [next(iter(s)) if isinstance(s, dict) else s for s in stages]


def _raw_pipeline_cfg() -> dict:
    return OmegaConf.to_container(OmegaConf.load(ETL_CFG), resolve=False)


def _event_cfg() -> dict:
    return yaml.safe_load(EVENT_CFG.read_text())


def _code_events() -> list[tuple[str, str, str]]:
    """(table, event, code expression) for every event in the shipped event config."""
    return [
        (table, event, spec["code"])
        for table, events in _event_cfg().items()
        if not table.startswith("_")
        for event, spec in events.items()
    ]


# --- pipeline config (ETL.yaml) ---------------------------------------------------------------------------


def test_pipeline_config_uses_meds_extract_08_keys():
    raw = _raw_pipeline_cfg()

    assert raw["MESSY_config_fp"] == "${oc.env:EVENT_CONVERSION_CONFIG_FP}"
    assert "event_conversion_config_fp" not in raw, "renamed to MESSY_config_fp in 0.7"
    assert raw["input_dir"] == "${oc.env:PRE_MEDS_DIR}"
    assert raw["output_dir"] == "${oc.env:MEDS_COHORT_DIR}"
    assert raw["shards_map_fp"] == "${output_dir}/metadata/.shards.json"
    # finalize_MEDS_metadata reads these two keys into dataset.json.
    assert raw["etl_metadata"]["dataset_name"] == "${oc.env:DATASET_NAME}"
    assert raw["etl_metadata"]["dataset_version"] == "${oc.env:DATASET_VERSION}"


def test_pipeline_stages_match_meds_extract_canonical_order():
    # The installed MEDS-Extract ships its canonical pipeline; extract_code_metadata must precede
    # merge_to_MEDS_cohort and the finalize stages must come last, or the run fails mid-pipeline.
    canonical = yaml.safe_load(
        files("MEDS_extract.configs").joinpath("_extract.yaml").read_text()
    )["stages"]

    assert _stage_names(_raw_pipeline_cfg()["stages"]) == _stage_names(canonical)


def test_every_pipeline_stage_is_registered():
    registered = {ep.name for ep in entry_points(group="MEDS_transforms.stages")}

    stages = _stage_names(_raw_pipeline_cfg()["stages"])
    assert set(stages) <= registered, f"unregistered: {set(stages) - registered}"
    assert "shard_events" not in stages, "replaced by convert_to_parquet in 0.7"


@pytest.mark.parametrize(
    ("env_value", "expected"), [(None, 10_000), ("123", 123)], ids=["default", "env"]
)
def test_subjects_per_shard_is_overridable_from_env(monkeypatch, env_value, expected):
    if env_value is None:
        monkeypatch.delenv("N_SUBJECTS_PER_SHARD", raising=False)
    else:
        monkeypatch.setenv("N_SUBJECTS_PER_SHARD", env_value)
    stages = OmegaConf.load(ETL_CFG).stages
    (split_stage,) = [
        s for s in stages if not isinstance(s, str) and "split_and_shard_subjects" in s
    ]

    assert split_stage.split_and_shard_subjects.n_subjects_per_shard == expected


# --- event config (event_configs.yaml) --------------------------------------------------------------------


def test_event_config_is_valid_messy():
    cfg = MessyConfig.load(EVENT_CFG)

    expected_tables = {t for t in _event_cfg() if not t.startswith("_")}
    assert set(cfg.table_prefixes) == expected_tables
    for table in cfg.event_tables:
        assert table.subject_id_node.referenced_columns == {"person_id"}, (
            table.input_prefix
        )


def test_event_config_has_no_legacy_syntax():
    raw = _event_cfg()
    text = EVENT_CFG.read_text()

    assert "subject_id_col" not in raw, "replaced by _defaults.subject_id in 0.7"
    assert raw["_defaults"] == {"subject_id": "$person_id"}
    assert "col(" not in text
    assert "time_format" not in text


@pytest.mark.parametrize(
    "dtype", [pl.String, pl.Int64], ids=["string_cols", "int_cols"]
)
@pytest.mark.parametrize(
    ("table", "event", "code"),
    [
        pytest.param(table, event, code, id=f"{table}-{event}")
        for table, event, code in _code_events()
        if "$" in code
    ],
)
def test_null_code_components_render_as_unk(table, event, code, dtype):
    """Every interpolated component must fall back to "UNK", as it did before MEDS-Extract 0.7.

    Since 0.7 a null component nulls the whole code and MEDS-Extract drops the row (e.g. the demo's 1,282
    observations without a resolvable concept), so a new code added without `?? 'UNK'` fails here.
    """
    cols = sorted(set(re.findall(r"\$(\w+)", code)))
    present = 7 if dtype == pl.Int64 else "X"
    raw = pl.DataFrame(
        {
            "subject_id": [1, 2],
            **{c: pl.Series([present, None], dtype=dtype) for c in cols},
        }
    )

    out = (
        EventConfig.parse(event, {"code": code, "time": None})
        .extract(raw.lazy(), f"{table}/{event}")
        .collect()
        .sort("subject_id")
    )

    assert code.startswith('f"') and code.endswith('"'), code
    template = code[2:-1]
    assert out["code"].to_list() == [
        re.sub(r"\{[^}]*\}", str(present), template),
        re.sub(r"\{[^}]*\}", "UNK", template),
    ]


# --- event-config pruning ---------------------------------------------------------------------------------


def test_prune_event_config_keeps_defaults_and_round_trips(tmp_path: Path):
    original = OmegaConf.load(EVENT_CFG)
    tables = [t for t in original if not str(t).startswith("_")]
    kept = {"person", "measurement", "note"}

    pruned, removed = prune_event_config(original, kept | {"codes"})

    assert list(pruned.keys()) == [
        "_defaults",
        *[t for t in tables if t in kept],
    ]
    assert removed == [t for t in tables if t not in kept]

    # __main__ saves the pruned config with OmegaConf for MEDS-Extract to re-read; the dftly expressions (with
    # their `??` and nested quotes) must survive that round trip unchanged.
    fp = tmp_path / "event_configs.yaml"
    OmegaConf.save(config=pruned, f=fp)
    reloaded = yaml.safe_load(fp.read_text())
    assert reloaded == {k: v for k, v in _event_cfg().items() if k in pruned}
    assert set(MessyConfig.load(fp).table_prefixes) == kept


def test_prune_event_config_without_missing_tables_removes_nothing():
    original = OmegaConf.load(EVENT_CFG)

    pruned, removed = prune_event_config(original, set(original.keys()))

    assert removed == []
    assert list(pruned.keys()) == list(original.keys())


# --- MEDS-Transforms command resolution -------------------------------------------------------------------


def test_this_environment_ships_the_pipeline_runner():
    scripts_dir = sysconfig.get_path("scripts")
    assert shutil.which(MEDS_PIPELINE_CMD, path=scripts_dir), scripts_dir


def test_pipeline_runner_resolves_from_this_environment_first():
    scripts_dir = sysconfig.get_path("scripts")
    elsewhere = os.pathsep.join(["/usr/bin", "/bin"])

    new_path = path_with_scripts_dir_first(MEDS_PIPELINE_CMD, elsewhere)

    assert new_path.split(os.pathsep) == [scripts_dir, "/usr/bin", "/bin"]
    resolved = Path(shutil.which(MEDS_PIPELINE_CMD, path=new_path))
    assert resolved.parent == Path(scripts_dir)


def _fake_scripts_dir(tmp_path: Path, command: str) -> Path:
    scripts = tmp_path / "bin"
    scripts.mkdir()
    exe = scripts / command
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    return scripts


def test_scripts_dir_is_moved_to_front_not_duplicated(tmp_path: Path):
    scripts = _fake_scripts_dir(tmp_path, "tool")
    path = os.pathsep.join(["/usr/bin", str(scripts), "/bin"])

    new = path_with_scripts_dir_first("tool", path, scripts_dir=scripts)

    assert new.split(os.pathsep) == [str(scripts), "/usr/bin", "/bin"]


def test_scripts_dir_without_command_leaves_path_unchanged(tmp_path: Path):
    scripts = _fake_scripts_dir(tmp_path, "other_tool")
    path = os.pathsep.join(["/usr/bin", "/bin"])

    assert path_with_scripts_dir_first("tool", path, scripts_dir=scripts) == path


def test_run_command_uses_argv_and_env_not_shell():
    def fake_run(cmd, capture_output, env):
        assert cmd == ["echo", "ok"]
        assert capture_output is True
        assert env["OMOP_MEDS_TEST_ENV"] == "1"
        return subprocess.CompletedProcess(cmd, 0, b"ok", b"")

    run_command(["echo", "ok"], env={"OMOP_MEDS_TEST_ENV": "1"}, runner_fn=fake_run)
