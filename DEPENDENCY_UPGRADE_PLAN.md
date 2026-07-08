# MEDS dependency upgrade implementation plan

## Goal

Upgrade `OMOP-MEDS` from the old combined `meds-transforms==0.2.4` / `meds~=0.3.3` stack to the split `MEDS-extract` stack while preserving current behavior.

Target runtime:

- `MEDS-extract ~= 0.6.2`
- `meds ~= 0.4.1`
- `MEDS-transforms` supplied by `MEDS-extract` unless direct imports remain
- no functional changes to OMOP-to-MEDS output except unavoidable upstream metadata/schema columns

## Source audit used for this plan

Checked on 2026-07-08:

| Source                                          |                           Commit inspected | Relevant finding                                                                                                                                                                     |
| ----------------------------------------------- | -----------------------------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Medical-Event-Data-Standard/ETL_MEDS_Template` | `acd66da2c6717ff349333e53556b0c4953e4cc95` | Current template depends on `MEDS-extract~=0.6`, uses `MEDS_transform-pipeline`, `output_dir`, `convert_to_subject_sharded`, `convert_to_MEDS_events`.                               |
| `Medical-Event-Data-Standard/MIMIC_IV_MEDS`     | `74bd0bde0d8d43914b8a32eafef3f5ded472b3d1` | Newer ETL pattern uses env vars + `MEDS_transform-pipeline ... --overrides output_dir=...`; subprocess is list-based, not shell-string based.                                        |
| `mmcdermott/MEDS_extract`                       | `a6c337fdeb1466a5f47890c92eded5a4b02cf754` | `0.6` MESSY config is dftly-based: old `col(...)`, list code construction, and `time_format` are removed.                                                                            |
| `Medical-Event-Data-Standard/meds`              | `80a6fc1f7dc7235c32c45be372c713e7d9a7210b` | `0.4.x` core schema keeps `subject_id`, `time`, `code`, optional `numeric_value`, `text_value`; code metadata requires `code`, `description`, `parent_codes`, extra columns allowed. |

## Current local impact points

- `pyproject.toml`
    - currently pins `meds-transforms==0.2.4`
    - currently pins `meds~=0.3.3`
- `src/OMOP_MEDS/__main__.py`
    - calls old `MEDS_transform-runner`
    - passes `pipeline_config_fp=...` and `hydra.searchpath=[pkg://MEDS_transforms.configs]`
    - exports `MEDS_COHORT_DIR`, not the new pipeline `output_dir` override pattern
- `src/OMOP_MEDS/configs/ETL.yaml`
    - extends old `_extract`
    - uses `cohort_dir`
    - stages include old `convert_to_sharded_events`
    - stage-specific config lives under `stage_configs`, which is not the 0.6 template style
- `src/OMOP_MEDS/configs/event_configs.yaml`
    - uses old MESSY syntax:
        - `col(name)`
        - list-based `code: [...]`
        - `time_format`
- Direct imports from old transform internals:
    - `MEDS_transforms.utils.get_shard_prefix` in `pre_meds.py`
    - `MEDS_transforms.utils.write_lazyframe` in `pre_meds_utils.py`
- Tests:
    - `tests/e2e_demo_test.py::test_local_e2e_demo_resources` is the useful local e2e smoke test.
    - `tests/e2e_demo_test.py::test_e2e` downloads PhysioNet data and should not be part of the normal validation command.

## Implementation phases

### Phase 1 — dependency metadata

1. Edit `pyproject.toml` dependencies:
    - replace `meds-transforms==0.2.4` with `MEDS-extract~=0.6.2`
    - replace `meds~=0.3.3` with `meds~=0.4.1`
    - keep existing non-MEDS deps unless resolver proves otherwise
2. Prefer not adding a separate `MEDS-transforms` dependency. If any direct import remains after Phase 2, either:
    - replace that import locally, or
    - declare `MEDS-transforms~=0.6.0` explicitly.
3. Run lock/update check:
    - `uv lock`
    - `uv pip install -e '.[tests]'`
    - `uv pip check`

Audit evidence to capture in PR notes:

```bash
uv pip freeze | grep -Ei 'meds|transforms|extract|dftly'
uv pip check
```

### Phase 2 — remove avoidable direct `MEDS_transforms` imports

1. Replace `get_shard_prefix(OMOP_input_dir, in_fp)` in `pre_meds.py` with a tiny local helper:
    - if `in_fp` is a file, use `in_fp.stem.split('.')[0]`
    - if `in_fp` is a directory, use `in_fp.name`
    - this matches current OMOP table paths produced by `get_table_path`
2. Replace `write_lazyframe(concept_df, concept_out_fp)` and `write_lazyframe(concept_relationship_df, concept_relationship_out_fp)` with `LazyFrame.sink_parquet(...)` or `collect().write_parquet(...)` only if sink fails for the target Polars version.

Acceptance:

```bash
rg 'MEDS_transforms' src tests
```

Expected result: no matches, unless `MEDS-transforms~=0.6.0` is kept as an explicit dependency.

### Phase 3 — migrate pipeline config to MEDS-extract 0.6

Edit `src/OMOP_MEDS/configs/ETL.yaml` to follow the 0.6 template:

1. Remove old defaults:

```yaml
defaults:
  - _extract
  - _self_
```

1. Use `output_dir` instead of `cohort_dir`:

```yaml
input_dir: ${oc.env:PRE_MEDS_DIR}
output_dir: ${oc.env:MEDS_COHORT_DIR}
shards_map_fp: ${output_dir}/metadata/.shards.json
```

1. Keep metadata:

```yaml
etl_metadata:
  dataset_name: ${oc.env:DATASET_NAME}
  dataset_version: ${oc.env:DATASET_VERSION}
```

1. Replace stages with the 0.6 names and inline stage config:

```yaml
stages:
  - shard_events:
      infer_schema_length: 999999999
  - split_and_shard_subjects:
      n_subjects_per_shard: ${oc.decode:${oc.env:N_SUBJECTS_PER_SHARD, 10000}}
  - convert_to_subject_sharded
  - convert_to_MEDS_events
  - merge_to_MEDS_cohort
  - extract_code_metadata
  - finalize_MEDS_metadata
  - finalize_MEDS_data
```

1. Keep parallel config, but allow serial override in `__main__.py`:

```yaml
parallelize:
  n_workers: ${oc.env:N_WORKERS,1}
  launcher: joblib
```

Acceptance:

```bash
uv run python - <<'PY'
from omegaconf import OmegaConf
from OMOP_MEDS import ETL_CFG
cfg = OmegaConf.load(ETL_CFG)
assert 'output_dir' in cfg
assert 'cohort_dir' not in cfg
assert 'convert_to_subject_sharded' in [list(x)[0] if isinstance(x, dict) else x for x in cfg.stages]
PY
```

### Phase 4 — migrate event config syntax

Edit `src/OMOP_MEDS/configs/event_configs.yaml` from old syntax to dftly syntax.

Rules:

| Old syntax                                                            | New syntax                                                           |
| --------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `code: MEDS_BIRTH` or `code: [MEDS_BIRTH]`                            | `code: MEDS_BIRTH`                                                   |
| `code: [GENDER, col(gender)]`                                         | `code: 'f"GENDER//{$gender}"'`                                       |
| `code: [col(preferred_vocabulary_name), col(preferred_concept_name)]` | `code: 'f"{$preferred_vocabulary_name}//{$preferred_concept_name}"'` |
| `code: [..., start]`                                                  | `code: 'f"...//start"'`                                              |
| `time: col(x)`                                                        | `time: $x` if parquet column is datetime/date typed                  |
| `time: null`                                                          | `time: null`                                                         |
| `numeric_value: col(x)`                                               | `numeric_value: $x`                                                  |
| `text_value: col(x)`                                                  | `text_value: $x`                                                     |
| `visit_occurrence_id: col(x)`                                         | `visit_occurrence_id: $x`                                            |
| `table_name: col(table_name)`                                         | `table_name: $table_name`                                            |
| `time_format: ...`                                                    | remove; only use `as "..."` if source is string                      |

Concrete conversions to apply:

- `person_birth_death.death_time.code`: `MEDS_DEATH`
- `person_birth_death.death_time.time`: `$date_of_death`
- `person_birth_death.age.code`: `MEDS_BIRTH`
- `person_birth_death.age.time`: `$date_of_birth`
- `person_birth_death.gender.code`: `'f"GENDER//{$gender}"'`
- `person.race.code`: `'f"RACE//{$race_concept_id}"'`
- `person.ethnicity.code`: `'f"ETHNICITY//{$ethnicity_concept_id}"'`
- all OMOP concept events: use `f"{$preferred_vocabulary_name}//{$preferred_concept_name}"`
- start/end events: append `//start` or `//end`
- `drug_exposure` currently uses `$drug_concept_id` in the code while most tables use `preferred_concept_name`; preserve existing behavior unless tests show it was a bug.

Acceptance:

```bash
rg 'col\(|time_format|code:\s*\[' src/OMOP_MEDS/configs/event_configs.yaml
```

Expected result: no matches.

### Phase 5 — migrate runner invocation

Edit `src/OMOP_MEDS/__main__.py` and `src/OMOP_MEDS/commands.py` toward the MIMIC pattern.

1. Change `run_command` to list-based subprocess execution:
    - `runner_fn(command_parts, capture_output=True, env={**os.environ, **env})`
    - no `shell=True`
    - keep stdout/stderr logging and `ValueError` on non-zero return
2. In `__main__.py`, construct env separately:

```python
env = {
    "DATASET_NAME": dataset_info.dataset_name,
    "DATASET_VERSION": f"{dataset_info.raw_dataset_version}:{PKG_VERSION}:OMOP_{dataset_info.omop_version}",
    "EVENT_CONVERSION_CONFIG_FP": str(Path(event_cfg_path).resolve()),
    "PRE_MEDS_DIR": str(pre_MEDS_dir.resolve()),
    "MEDS_COHORT_DIR": str(MEDS_cohort_dir.resolve()),
}
```

1. Use new command:

```python
command_parts = ["MEDS_transform-pipeline", str(ETL_CFG.resolve())]
```

1. Add overrides:

```python
overrides = [f"output_dir={MEDS_cohort_dir.resolve()!s}"]
if cfg.get("do_overwrite") is not None:
    overrides.append(f"do_overwrite={cfg.do_overwrite}")
if cfg.get("seed") is not None:
    overrides.append(f"seed={cfg.seed}")
if int(os.getenv("N_WORKERS", 1)) <= 1:
    overrides.append("~parallelize")
if overrides:
    command_parts.extend(["--overrides", *overrides])
```

1. Preserve `stage_runner_fp`, but use the new flag form if supported by installed CLI:
    - first try `--stage_runner_fp=<path>` as in current MIMIC
    - if tests fail due CLI spelling, check `MEDS_transform-pipeline --help` and use exact supported option
2. Remove old additions:
    - `pipeline_config_fp=...`
    - quoted `hydra.searchpath=[pkg://MEDS_transforms.configs]`
    - `MEDS_transform-runner`

Acceptance:

```bash
uv run python -m OMOP_MEDS.__main__ --version
uv run python - <<'PY'
from OMOP_MEDS.commands import run_command
import subprocess, os

def fake(cmd, capture_output, env):
    assert isinstance(cmd, list)
    assert env["X"] == "1"
    return subprocess.CompletedProcess(cmd, 0, b"ok", b"")
run_command(["echo", "ok"], env={"X":"1"}, runner_fn=fake)
PY
```

### Phase 6 — MEDS 0.4.1 compatibility checks

1. Add a focused test that validates local e2e output against `meds` schemas:
    - data shards contain at least `subject_id`, `time`, `code`
    - `subject_id` is int64
    - `time` is datetime/date castable to `timestamp(us)`
    - `code` is string and non-null
    - if present, `numeric_value` is float-castable
    - `metadata/codes.parquet` contains `code`, `description`, `parent_codes`
2. Keep existing CLI scripts unchanged:
    - `MEDS_extract-OMOP`
    - `OMOP_MEDS`

Acceptance:

```bash
uv run pytest tests/test_* -q
uv run pytest tests/e2e_demo_test.py::test_local_e2e_demo_resources -q
```

### Phase 7 — test policy for downloaded e2e

Do not delete `tests/e2e_demo_test.py::test_e2e`.

Routine validation should exclude it:

```bash
uv run pytest -q -k 'not test_e2e'
```

Optional improvement: add a `@pytest.mark.download` marker to `test_e2e` and configure `pytest.ini` / `pyproject.toml` markers. Do not add an unconditional skip unless the maintainer wants CI never to run the downloaded test.

Full network validation remains:

```bash
uv run pytest tests/e2e_demo_test.py::test_e2e -q
```

### Phase 8 — regression comparison

Before changing dependencies, capture a baseline from current code if the old env still resolves:

```bash
BASE=$(mktemp -d)
uv run pytest tests/e2e_demo_test.py::test_local_e2e_demo_resources -q
```

After migration, compare the important invariants on local demo output:

- same or explainably equivalent subject count
- same or explainably equivalent event count
- same unique code set after accounting for `MEDS-extract` 0.6 `code_components` / `source_block` additions
- metadata files exist:
    - `metadata/dataset.json`
    - `metadata/codes.parquet`
    - `metadata/subject_splits.parquet`

Add one small assertion test for those invariants rather than snapshotting entire parquet files.

## Final validation command set

Run these before considering the migration done:

```bash
uv lock
uv pip install -e '.[tests]'
uv pip check
uv run python -m OMOP_MEDS.__main__ --version
uv run pytest -q -k 'not test_e2e'
```

If time/network allows:

```bash
uv run pytest tests/e2e_demo_test.py::test_e2e -q
```

## Update agents.md

Update agents.md to align with the new package

## Known risks and cheapest mitigations

| Risk                                                                                            | Mitigation                                                                                                                                |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| dftly expression quoting breaks YAML parsing                                                    | Single-quote f-string expressions in YAML.                                                                                                |
| `time` columns are already parquet datetime/date, so adding `as` casts may regress              | Do not add casts unless a test proves a source column is string.                                                                          |
| `MEDS-extract` 0.6 emits extension columns (`code_components`, `source_block`, `code_template`) | Allow them; MEDS 0.4 permits extra data/code metadata columns.                                                                            |
| Old local `finish_codes_metadata` may conflict with upstream `extract_code_metadata` output     | Keep only if it preserves OMOP parent metadata; otherwise make it additive and schema-compatible (`code`, `description`, `parent_codes`). |
| Downloaded e2e is slow/flaky                                                                    | Exclude from routine test command; keep test available.                                                                                   |

## Definition of done

- Dependencies resolve with `MEDS-extract~=0.6.2` and `meds~=0.4.1`.
- No old event config syntax remains.
- Pipeline runs through `MEDS_transform-pipeline`, not `MEDS_transform-runner`.
- Existing non-download tests pass.
- Local checked-in demo e2e passes.
- Downloaded e2e test remains in the repository.
- Public CLI and config surface remain backward-compatible for existing users.
