import subprocess

from omegaconf import OmegaConf

from OMOP_MEDS import ETL_CFG, EVENT_CFG
from OMOP_MEDS.commands import run_command


def test_meds_extract_06_pipeline_config_keeps_large_dataset_limits():
    cfg = OmegaConf.load(ETL_CFG)
    raw = OmegaConf.to_container(cfg, resolve=False)
    stages = {
        next(iter(stage.keys())): next(iter(stage.values()))
        for stage in raw["stages"]
        if isinstance(stage, dict)
    }

    assert raw["output_dir"] == "${oc.env:MEDS_COHORT_DIR}"
    assert raw["shards_map_fp"] == "${output_dir}/metadata/.shards.json"
    assert stages["shard_events"]["row_chunksize"] == 20_000_000_000
    assert stages["shard_events"]["infer_schema_length"] == 999_999_999
    assert (
        stages["split_and_shard_subjects"]["n_subjects_per_shard"]
        == "${oc.decode:${oc.env:N_SUBJECTS_PER_SHARD, 10000}}"
    )
    assert "convert_to_subject_sharded" in raw["stages"]
    assert "convert_to_sharded_events" not in raw["stages"]


def test_event_config_uses_dftly_syntax_not_legacy_col_syntax():
    text = EVENT_CFG.read_text()

    assert "col(" not in text
    assert "time_format" not in text
    assert "code:" in text
    assert 'f"{$preferred_vocabulary_name}//{$preferred_concept_name}"' in text


def test_run_command_uses_argv_and_env_not_shell():
    def fake_run(cmd, capture_output, env):
        assert cmd == ["echo", "ok"]
        assert capture_output is True
        assert env["OMOP_MEDS_TEST_ENV"] == "1"
        return subprocess.CompletedProcess(cmd, 0, b"ok", b"")

    run_command(["echo", "ok"], env={"OMOP_MEDS_TEST_ENV": "1"}, runner_fn=fake_run)
