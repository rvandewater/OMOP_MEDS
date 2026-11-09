# AGENTS.md

Project instructions for Pi (`pi.dev`) and other coding agents working in this repository.

## Purpose

This repository is a Python ETL package that converts OMOP Common Data Model datasets into MEDS format. Treat changes as clinical-data infrastructure work: correctness, reproducibility, privacy, and scalability matter more than cleverness or broad rewrites.

Pi reads this file as project context. Keep it in the repository root and run `/reload` in Pi after editing it.

## Operating principles

- Follow the user's request first, then these instructions, then existing repository conventions.
- Prefer small, reviewable changes. Do not reformat unrelated files or rewrite stable code without a direct reason.
- Before editing, inspect the relevant code, configs, and tests. Do not guess schema behavior from memory.
- When a task spans multiple files, briefly state the approach, then implement and verify.
- Never create commits, tags, releases, or publish packages unless the user explicitly asks.
- Never add real clinical data, credentials, tokens, local paths, generated ETL outputs, or large files to version control.
- Treat OMOP input data, MEDS output data, notes, and patient-level identifiers as sensitive even when files look like demos.

## Repository map

- `pyproject.toml` - package metadata, Python requirement, dependencies, optional extras, console scripts.
- `src/OMOP_MEDS/configs/event_configs.yaml` - the MESSY spec registered as `OMOP` (`meds-extract-run spec=OMOP`): MEDS events from the pre-MEDS tables, plus the `etl:` block (extension stages, shard size, dataset metadata) and the demo's `sources:`.
- `src/OMOP_MEDS/stages.py` - the MEDS-Extract extension stages `omop_pre_meds` (pre-stage) and `omop_codes_metadata` (post-stage), and their options.
- `src/OMOP_MEDS/__main__.py` - the deprecated `OMOP_MEDS` / `MEDS_extract-OMOP` commands, which translate the old runner's arguments into `meds-extract-run spec=OMOP`.
- `src/OMOP_MEDS/pre_meds.py` - pre-MEDS OMOP wrangling orchestration.
- `src/OMOP_MEDS/pre_meds_utils.py` - table joins, concept selection, datetime handling, metadata helpers.
- `src/OMOP_MEDS/pre_meds_data_loader.py` - raw table loading, sharded table handling, batching utilities.
- `src/OMOP_MEDS/utils.py` - the code metadata (`codes.parquet`) written by `omop_codes_metadata`.
- `src/OMOP_MEDS/dataset.yaml` - the default OMOP CDM version (`OMOP_VERSION`, else 5.3).
- `src/OMOP_MEDS/configs/pre_MEDS/omop_<version>.yaml` - pre-MEDS table preprocessing for one OMOP CDM version (5.3, 5.4, 5.5), chosen by the `omop_pre_meds` option `omop_version`, else `OMOP_VERSION`.
- `src/OMOP_MEDS/configs/pre_MEDS/common.yaml` - pre-MEDS settings shared by every OMOP version.
- `src/OMOP_MEDS/omop_versions.py` - supported OMOP versions and loading their pre-MEDS configs. Table schemas, CDM table lists and reading input tables come from the `omop_schema` package.
- `tests/` - unit, utility, batching, datetime, NLP, patient-link, and end-to-end tests.
- `tests/demo_resources/` - small checked-in demo OMOP CSVs for local tests. Prefer these over network downloads.

## Environment setup

Use an isolated environment. Python `>=3.11.4,<3.14` is required (the range MEDS-Extract 0.8 supports); CI exercises Python 3.11, 3.12 and 3.13.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev,tests]'
```

Optional extras:

```bash
python -m pip install -e '.[local_parallelism]'
python -m pip install -e '.[slurm_parallelism]'
python -m pip install -e '.[large_data]'
```

Do not bump pinned or compatibility-sensitive dependencies unless the task requires it. If dependencies change, update `pyproject.toml`, note why, and run relevant tests.

## Core commands

Run targeted checks first, then broader checks when appropriate.

```bash
# Fast targeted test
HYDRA_FULL_ERROR=1 pytest tests/test_build_preferred_event_datetime.py -q

# Local checked-in demo smoke test, no network download
HYDRA_FULL_ERROR=1 pytest tests/e2e_demo_test.py::test_local_e2e_demo_resources -q

# Full project test command used by CI
HYDRA_FULL_ERROR=1 pytest src/ tests/ -v --doctest-modules --cov=src --junitxml=junit.xml -s

# Code quality hooks
pre-commit run --all-files

# Build package artifacts locally
python -m build
```

Notes:

- Prefer `test_local_e2e_demo_resources` for ETL smoke tests because it uses checked-in demo files.
- Tests run the ETL as users do, with `meds-extract-run spec=OMOP` from the test environment (`tests/omop_run.py`).
- `tests/e2e_demo_test.py::test_e2e` may download demo data from PhysioNet. Do not run network/downloading tests unless the task requires it or the user permits it.
- If `pre-commit` changes files, inspect the diff and rerun the relevant hook or test.

## Running the CLI safely

The ETL is the MEDS-Extract spec registered as `OMOP`. A minimal local run looks like:

```bash
OMOP_VERSION=5.3 meds-extract-run spec=OMOP output_dir=/tmp/omop_meds_output \
  do_download=false input_dir=/path/to/omop_input 'skip_stages=[sumeds_enrich]'
```

The deprecated `OMOP_MEDS` and `MEDS_extract-OMOP` commands translate the old runner's arguments into this
command (`src/OMOP_MEDS/__main__.py`). Stage options are set for one run with
`"overrides=['++stage_cfg.<option>=<value>']"` (see `src/OMOP_MEDS/stages.py`).

Safety rules:

- Never run destructive ETL commands against user data paths without explicit approval.
- `"overrides=['do_overwrite=True']"` can remove existing outputs. Use temporary directories for tests and examples.
- Avoid `dataset_key=demo` without `do_download=false` unless the task requires a download.
- Do not print raw rows from real patient tables, especially `note_text` or patient-level identifiers.
- Prefer small `"overrides=['++stage_cfg.limit_subjects=<n>']"` debug runs when validating pipeline behavior on
    non-demo data.

## ETL architecture and invariants

The pipeline is `meds-extract-run spec=OMOP`, in four phases:

1. Optional download of the MIMIC-IV OMOP demo (`dataset_key=demo`, the spec's `sources:`).
2. The `omop_pre_meds` pre-stage: pre-MEDS wrangling from the OMOP tables in `input_dir` to Parquet tables under
    `OUTPUT_DIR/omop_pre_meds`, which MEDS-Extract reads instead of the raw tables. It writes every table the spec
    reads, empty when the input has none, so the one registered spec fits every OMOP dataset.
3. MEDS-Extract's canonical stages: event conversion and sharding into `OUTPUT_DIR/data` and `metadata/`.
4. Post-stages: `omop_codes_metadata` writes `codes.parquet` from the pre-MEDS vocabulary tables, then suMEDS's
    `sumeds_enrich` enriches it in place with the rules registered as `OMOP` and records that in
    `metadata/dataset.json`. A failed enrichment is recorded without failing the run unless the stage's `strict`
    option is set; `skip_stages=[sumeds_enrich]` leaves it out. Tests run it offline, against a local pack
    fixture (`tests/test_run_with_enrichment.py`).

Preserve these invariants:

- OMOP versions 5.3, 5.4 and 5.5 are supported. When adding table behavior, check every version's schema and pre-MEDS config.
- `person_id` is the subject identifier; `visit_occurrence_id` is the visit/admission identifier where relevant.
- Raw table inputs may be single files or directories of shards, and may be Parquet, CSV, or gzipped CSV.
- Pre-MEDS table output is Parquet and is later interpreted by `event_configs.yaml`.
- `preferred_vocabulary_name` and `preferred_concept_name` drive many MEDS event codes. Preserve their semantics.
- Default behavior prefers mapped concepts; only prefer source concepts when `prefer_source=True` or the task explicitly requires it.
- Do not silently drop supported OMOP tables. Missing input tables can be skipped with warnings; unexpected present-but-unhandled tables should remain visible in logs.
- Preserve `table_name` in pre-MEDS outputs used by event conversion.
- Notes use `preferred_time` so last-edit timestamps can prevent temporal leakage. Do not regress note datetime handling.
- Date-only fallbacks may be promoted to end-of-day for comparison with full datetimes; keep tests around this behavior.
- Visit events may include `care_site_name` when `care_site` is available. Missing `care_site` should not break the run.

## Polars and performance rules

This package is intended to handle large clinical tables. Avoid patterns that work only on tiny data.

- Prefer Polars `LazyFrame` operations, `scan_*`, streaming-friendly transformations, and `sink_parquet`.
- Avoid eager `.collect()` on large tables unless the existing design already requires it or the data is known to be small metadata.
- Never convert large Polars frames to pandas for implementation convenience.
- Avoid Python row loops and `map_elements` on large columns. Use Polars expressions instead.
- Maintain sharded/batched loading behavior for large tables such as `measurement`, `observation`, and `note`.
- Preserve explicit casting of OMOP IDs and datetime columns; schema drift is common in real OMOP exports.
- Keep memory use predictable. When adding a join, consider row counts, join keys, selected columns, and whether the right side should be projected first.
- Do not increase row group sizes, chunk sizes, worker counts, or default batching thresholds without a measured reason.

## Hydra and configuration rules

- Keep user-facing defaults in YAML whenever possible; keep Python code responsible for orchestration and validation.
- Use Hydra/OmegaConf interpolation patterns already present in the repo for environment-variable overrides.
- When adding a new runtime option, add it to the stage's defaults in `stages.py` (MEDS-Extract rejects spec options a stage does not declare), document it in `README.md` if user-facing, and add or adjust tests.
- When adding or changing a pre-MEDS table:
    1. Update `configs/pre_MEDS/omop_<version>.yaml` for every OMOP version whose CDM has the table.
    2. Ensure the `omop_schema` schema of each OMOP version has the table (`get_table_names()`).
    3. Add or update entries in `event_configs.yaml` if the table should produce MEDS events.
    4. Add tests using `tests/demo_resources` or synthetic temporary Parquet/CSV data.
- Each OMOP version has its own pre-MEDS config file. Make a change in every version where the CDM table is the same; `tests/test_omop_versions.py` fails when tables the CDM left unchanged are configured differently, or a configured column is not a CDM field.
- Do not hard-code local dataset names, usernames, passwords, or private URLs. Use environment variables and config overrides.

## Testing guidance

Choose tests based on the risk of the change.

- Datetime logic: run `tests/test_build_preferred_event_datetime.py` and add edge cases for strings, dates, datetimes, nulls, and override ordering.
- Patient/person filtering: run `tests/test_get_patient_link_join_on_visit.py`.
- Concept joins and batch safety: run `tests/test_join_concept_batch_safety.py`.
- Sharded or large-table loading: run `tests/test_sharded_table_data_loader.py`.
- NLP note feature extraction: run `tests/test_nlp_feature_extraction.py` and avoid exposing raw note contents in logs.
- End-to-end or config changes: run `tests/e2e_demo_test.py::test_local_e2e_demo_resources`.
- OMOP version configs and schemas: run `tests/test_omop_versions.py`.
- Public examples/docstrings: run the CI-style doctest command over `src/` and `tests/`.

When adding tests:

- Prefer tiny, deterministic data built inside the test or copied from `tests/demo_resources`.
- Use `tmp_path` or `TemporaryDirectory`; do not write outputs into the repository tree.
- Assert schemas, row counts, code values, timestamps, and important metadata outputs, not just "command succeeded".
- Add regression tests for bugs before or with the fix.
- Keep network-dependent tests separate from local deterministic tests.

## Code style

- Follow existing style and run pre-commit. The repo uses Ruff formatting/linting through pre-commit.
- Use clear names that match OMOP/MEDS terminology.
- Add type hints for new public helpers and non-trivial internal functions.
- Keep functions focused. Extract helpers when a transformation becomes hard to test.
- Prefer `pathlib.Path` over string path manipulation.
- Use the logger style already present in the file being edited.
- Keep error messages actionable: include table name, column name, OMOP version, and path when useful.
- Do not catch broad exceptions unless you re-raise or log enough context to debug data/schema issues.
- Do not add comments that restate obvious code. Comments should explain clinical, schema, performance, or temporal-leakage reasoning.

## Documentation rules

Update `README.md` when changing user-facing behavior, install extras, CLI flags, defaults, supported OMOP versions, or recommended workflows.

For docs/examples:

- Use temporary or placeholder paths, not private filesystem paths.
- Prefer local demo resources for examples.
- Mention safety implications for `prefer_source`, `join_on_visit`, note processing, batching, and `do_overwrite` when relevant.
- Keep examples compatible with Python 3.11+.

## Data privacy and security

- Assume real OMOP tables can contain PHI, even columns that look administrative.
- Do not upload, paste, summarize, or log real patient-level data outside the local environment.
- Never commit generated `raw_input`, `omop_pre_meds`, `MEDS_cohort`, `.logs`, downloaded datasets, or notebooks with outputs from real data.
- Never add credentials to YAML. Use `${oc.env:...}` or documented environment variables.
- Keep `detect-private-key` and other pre-commit security hooks enabled.
- If a task requires inspecting data, use schemas, column names, row counts, and synthetic samples wherever possible.

## Dependency and packaging rules

- This is a library/CLI package. Avoid adding heavy dependencies for small utilities.
- Keep Python 3.11 compatibility unless the project intentionally raises the minimum.
- Do not change package name, console script names, or public config paths without a migration note.
- If changing build or release config, run `python -m build` and inspect generated metadata locally.
- Publishing to PyPI and creating GitHub releases is release work; do not attempt it unless explicitly requested.

## Pull request / final response checklist

Before reporting completion, verify and summarize:

- What files changed and why.
- What commands were run and their results.
- What commands were not run and why.
- Any risks, assumptions, or follow-up work.
- Which OMOP versions (5.3, 5.4, 5.5) changed behavior.
- Whether docs/config/tests were updated when needed.

## Common change recipes

### Add support for a new OMOP source column

1. Confirm the column exists in OMOP 5.3, 5.4 or 5.5 (the `omop_schema` schemas follow the CDM specification), or list it as a known extension in `NON_CDM_COLUMNS` in `tests/test_omop_versions.py`.
2. Add the column to the relevant `output_data_cols` only when it is needed downstream.
3. Ensure the column survives metadata-column dropping if required.
4. Add event config usage only if it should become a MEDS event field.
5. Add a test with the column present and, when realistic, absent.

### Add a new event type from an existing table

1. Verify the pre-MEDS output already contains all required columns.
2. Add a new block under the table in `event_configs.yaml`.
3. Use `preferred_vocabulary_name` and `preferred_concept_name` where concept mapping should drive the event code.
4. Include `visit_occurrence_id`, `table_name`, numeric values, units, text values, or link IDs only when meaningful.
5. Run the local end-to-end demo smoke test.

### Change datetime behavior

1. Locate the source OMOP date/datetime columns and their types.
2. Preserve temporal-leakage protections, especially for notes and last-edit timestamps.
3. Add tests for nulls, date-only values, full datetimes, strings, and override order.
4. Run targeted datetime tests and an end-to-end smoke test.

### Change large-table processing

1. Inspect `ShardedTableDataLoader` and current batching config first.
2. Preserve support for file and directory inputs.
3. Test `auto`, `per_shard`, `by_shards`, and row-target behavior when relevant.
4. Avoid adding full-table materialization.
5. Run sharded loader tests and a local e2e smoke test.
