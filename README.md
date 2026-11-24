<div align="center">
  <img src="https://github.com/rvandewater/OMOP_MEDS/blob/main/static/omop_to_meds_logo.svg?raw=true"
alt="omop-to-meds logo" height="300">
</div>

# MEDS OMOP ETL

[![PyPI - Version](https://img.shields.io/pypi/v/OMOP_MEDS)](https://pypi.org/project/OMOP_MEDS/)
[![codecov](https://codecov.io/gh/rvandewater/OMOP_MEDS/graph/badge.svg?token=RW6JXHNT0W)](https://codecov.io/gh/rvandewater/OMOP_MEDS)
[![tests](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.11%20%7C%203.12%20%7C%203.13-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/OMOP_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/OMOP_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/OMOP_MEDS.svg)](https://github.com/rvandewater/OMOP_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/940565218.svg)](https://doi.org/10.5281/zenodo.19398854)
[![MEDS v0.4](https://img.shields.io/badge/MEDS-0.4-blue)](https://medical-event-data-standard.github.io/)

An ETL pipeline for transforming Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM)
datasets into MEDS using MEDS-Extract.
We gratefully acknowledge the developers of the first OMOP MEDS ETL, from which we took inspiration,
which can be found here: https://github.com/Medical-Event-Data-Standard/meds_etl.

We support OMOP CDM 5.3, 5.4 and 5.5 datasets (see [OMOP CDM versions](#omop-cdm-versions)). Earlier versions
might work but are not tested and are perhaps (?) not used in practice anymore. Please open pull requests if you
want to add support for earlier versions.

- More information about OMOP can be found here: https://ohdsi.github.io/CommonDataModel/
- More information about MEDS can be found here: https://medical-event-data-standard.github.io/

For reading in OMOP in the right schema, we use the `omop-schema` package (developed for this ETL but can be used
elsewhere), which can be found here:
https://github.com/rvandewater/omop_schema.
It holds the table schemas of OMOP CDM 5.3, 5.4 and 5.5, checked against the official CDM specification, and
reads the input tables (`omop_schema.scan.scan_table`):

- CSV, gzipped CSV and Parquet files, and directories of their shards, are supported.
- Column names are lowercased before the schema is applied, so uppercase headers (as SQL Server exports write them)
    get their types.
- Columns outside the schema, such as the `xtn_note_last_edit_*` columns notes need for leakage-safe times, are
    kept, also when a table is a directory of Parquet shards.
- DATE columns written with a midnight time (`2005-05-07 00:00:00`) are read as dates.
- Values must fit their OMOP type in CSV files and single Parquet files. Directories of Parquet shards are cast
    permissively (a value that does not fit becomes null), and every schema column is added to them, because
    large exports drift in type from shard to shard.

If your OMOP schema is non-standard (but still approximately OMOP), you should be able to use the `omop-schema` package to define your own schema and use it in this ETL.

## Setup

Requires Python `>=3.11.4,<3.14`, the range MEDS-Extract 0.8 supports: its Hydra command line fails on Python 3.14
([MEDS_extract#248](https://github.com/mmcdermott/MEDS_extract/issues/248)), and its archive extraction on 3.11.0–3.11.3
([MEDS_extract#217](https://github.com/mmcdermott/MEDS_extract/issues/217)). The MEDS conversion runs on MEDS-Extract 0.8.1
(the first with extension stages, which this ETL is made of) and MEDS-Transforms 0.7, with suMEDS 0.6.2.

First install the package, which brings [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) and
[suMEDS](https://github.com/rvandewater/SumMEDS):

```bash
pip install OMOP_MEDS
```

The ETL is one [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) spec, registered as `OMOP`, so it runs with
MEDS-Extract's own command:

```bash
export OMOP_VERSION="5.4" # 5.3, 5.4 or 5.5
meds-extract-run spec=OMOP output_dir=$MEDS_DIR do_download=false input_dir=$OMOP_DIR \
	dataset_version=1.0 "overrides=['etl_metadata.dataset_name=MY_OMOP_DATASET']"
```

`$OMOP_DIR` holds the OMOP CDM tables (CSV, gzipped CSV or Parquet files, or directories of their shards). This
writes the MEDS dataset to `$MEDS_DIR` (`data/` and `metadata/`, per the
[MEDS standard](https://medical-event-data-standard.github.io/)). `dataset_version=` and
`etl_metadata.dataset_name` are recorded in `metadata/dataset.json`; without them, a build is named `OMOP` with
version `unknown:<OMOP_MEDS version>`. The run has three OMOP-specific steps around MEDS-Extract's canonical
stages (`src/OMOP_MEDS/stages.py`), declared in the spec's `etl:` block
([`src/OMOP_MEDS/configs/event_configs.yaml`](src/OMOP_MEDS/configs/event_configs.yaml)):

1. `omop_pre_meds` joins the OMOP tables to their concepts and writes the pre-MEDS tables to
    `$MEDS_DIR/omop_pre_meds`, where MEDS-Extract reads them. Your OMOP data is never modified.
2. `omop_codes_metadata` writes `metadata/codes.parquet` from the source's own vocabulary tables (see
    [Code metadata](#code-metadata-codesparquet)).
3. `sumeds_enrich` (from suMEDS) adds official vocabulary titles and ancestors to `metadata/codes.parquet` (see
    [Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)).

Each stage keeps its output under `$MEDS_DIR/<stage>`; `do_cleanup_intermediates=true` deletes those once the
run has succeeded. A run that is started again resumes, and skips the stages it has finished. To convert again
from scratch, use a new `output_dir`.

Quote arguments with brackets (`'skip_stages=[sumeds_enrich]'`): shells such as zsh would otherwise expand them.

To try with the MIMIC-IV OMOP demo dataset (this downloads PhysioNet's ~11 MB project zip into
`$MEDS_DIR/.meds_extract_run/raw_input`, or `download_dest_dir=`):

```bash
meds-extract-run spec=OMOP output_dir=/path/to/demo_meds dataset_key=demo
```

### Enrichment, and sites without internet

suMEDS enrichment is on by default and does not need an Athena download: its vocabulary data (ontology packs)
comes from GitHub and is cached. It never fails the run: if it fails, for example on a hospital server without
internet access, `metadata/codes.parquet` stays as the ETL wrote it and the failure is recorded. Whether a build
was enriched is recorded in `metadata/dataset.json`:

```bash
python -c "import json, sys; print(json.load(open(sys.argv[1]))['sumeds_enrichment']['enriched'])" \
	$MEDS_DIR/metadata/dataset.json
```

(`true`, or `false` with the `error` and the `retry` command.) The warning is written to
`$MEDS_DIR/.logs/pipeline.log`, not to the console. Running `meds-extract-run` again does not retry a failed
enrichment (the stage is done); to enrich a build afterwards, with the rules and options of the run, and update
`dataset.json` to match:

```bash
suMEDS-enrich $MEDS_DIR
```

- **Without enrichment:** add `'skip_stages=[sumeds_enrich]'`. `suMEDS-enrich $MEDS_DIR --rules OMOP` enriches
    such a build later.

- **Fail instead:** `"overrides=['++stage_cfg.strict=true']"` makes a failed enrichment fail the run.

- **Compute nodes without internet (e.g. HPC):** fill the pack cache on a machine with internet, such as a login
    node, with the same cache directory, then run offline, so that nothing waits on the network:

    ```bash
    export XDG_CACHE_HOME=/shared/project/cache # the same on the login and the compute nodes
    suMEDS-enrich --prefetch --rules OMOP # on the login node: downloads the pinned pack release (~30 MB)
    meds-extract-run spec=OMOP output_dir=$MEDS_DIR do_download=false input_dir=$OMOP_DIR \
    	"overrides=['++stage_cfg.offline=true']" # on a compute node
    ```

    A server that never has internet can use a copy of the
    [sumeds-ontologies](https://github.com/rvandewater/sumeds-ontologies) packs directory instead:
    `SUMEDS_ONTOLOGY_SOURCE=/path/to/sumeds-ontologies/ontologies`, or
    `suMEDS-enrich $MEDS_DIR --ontology-source /path/to/sumeds-ontologies/ontologies --offline` afterwards.

The options of the `sumeds_enrich` stage (`strict`, `offline`, `ontology_source`, `loinc_hierarchy`, ...) are
those of `suMEDS-enrich`, spelled with underscores; set them for one run with
`"overrides=['++stage_cfg.<option>=<value>']"`. See the
[suMEDS documentation](https://github.com/rvandewater/SumMEDS/blob/main/docs/usage.md#enrich-inside-a-meds-extract-etl).

## Differences with the original meds_etl_omop

This package is designed as a more flexible and configurable alternative to the original `meds_etl_omop` package.
We make a few important choices that have impact on your downstream training and task definitions:

- We use the mapped concepts by default, which are more standardized across datasets and, for large, health systems can
    be more clean, especially if you are working with a limited tokenizer on a large dataset.
    You can still use the source concepts by setting `++prefer_source=True`.
- We use more tables than in the original `meds_etl_omop` package, which can lead to more complete patient histories.
    Watch for potential information leakage. You can change your table configs in
    `configs/pre_MEDS/omop_<version>.yaml` and `event_configs.yaml`.
- This package is more resource intensive, please adjust your `n_shards` and watch your memory usage.
- OMOP requires an event's date but not its datetime. An event is timed by its `*_datetime` column, or, when that
    is empty, at the end of its `*_date` (23:59:59): the latest time it can have happened, so it is never placed
    before something it may have followed. Deaths and notes are timed the same way. Events of the same code and
    value on the same date then become identical rows, which MEDS keeps once.

## Different package versions

We have different versions of the ETL pipeline, which are designed for different use cases:

```bash
pip install OMOP_MEDS[dev] # for development and testing
pip install OMOP_MEDS[tests] # for running tests
pip install OMOP_MEDS[local_parallelism] # for local parallelization with hydra-joblib-launcher
pip install OMOP_MEDS[slurm_parallelism] # for parallelization with slurm using hydra-submitit-launcher
pip install OMOP_MEDS[large_data] # for handling large datasets with polars (rt64), more than 3.2B rows
```

Run routine tests (skipping the two end-to-end tests that download data; drop `-m` to run everything):

```bash
pytest -q -m 'not physionet and not synthea'
```

The two end-to-end tests run the full ETL over downloaded data:

- `pytest -m synthea` uses OHDSI's [Synthea27Nj](https://github.com/OHDSI/EunomiaDatasets) sample: 28 synthetic
    subjects in OMOP CDM 5.4 (Apache-2.0), pinned and SHA-256 verified, cached in `~/.cache/OMOP_MEDS/test-data`
    (override with `SYNTHEA_OMOP_CACHE_DIR`). CI runs it in its own job on every pull request.
- `pytest -m physionet` downloads the MIMIC-IV OMOP demo (OMOP CDM 5.3) from PhysioNet. CI runs it on pushes to
    `main`, nightly and on demand, but not on pull requests, because PhysioNet is slow and unreliable from CI
    runners.

## Pre-MEDS settings

The options of the `omop_pre_meds` stage are set for one run with `overrides=`, e.g. a debug run on 50 persons:

```bash
meds-extract-run spec=OMOP output_dir=$MEDS_DIR do_download=false input_dir=$OMOP_DIR \
	"overrides=['++stage_cfg.limit_subjects=50','++stage_cfg.prefer_source=true']"
```

- `omop_version`: the OMOP CDM version of the input (5.3, 5.4 or 5.5); by default the `OMOP_VERSION` environment
    variable, else 5.3.
- `limit_subjects`: Limit the number of subjects to process.
- `prefer_source`: Set to `true` to prefer source concepts over mapped concepts.
- `join_on_visit`: Set to `true` to keep only persons with at least one visit.
- `tables_to_ignore`: Tables that are not converted, without a warning.

Pre-meds batching settings. This is relevant if (some of) your input tables are very large, and you want to process
them in batches. This can be useful to reduce memory usage, but it also increases the runtime, so use with caution.
The batching settings are as follows:

- `pre_meds_chunked_tables`: Tables eligible for batched pre-MEDS processing.
- `pre_meds_batching_row_threshold`: Row count threshold for batching.
- `pre_meds_batch_mode`: `auto`, `per_shard`, `by_shards`, or `by_rows`.
- `pre_meds_batch_size_shards`: Shards per batch in `by_shards` mode.
- `pre_meds_batch_input_rows`: Max rows per batch in `by_rows` mode.

Their defaults are in [`src/OMOP_MEDS/stages.py`](src/OMOP_MEDS/stages.py) (`PRE_MEDS_DEFAULTS`). To change them
for good, copy the spec and set them on the stage in its `etl:` block (`- omop_pre_meds: {prefer_source: true}`),
then run `meds-extract-run spec=./my_spec.yaml ...`.

Every table the spec converts is written by `omop_pre_meds`, without rows when the input has none, so the one spec
fits every OMOP dataset. A dataset without notes therefore still has an (empty) `link_id` column, like one with
notes.

### Upgrading from the `OMOP_MEDS` runner

The `OMOP_MEDS` and `MEDS_extract-OMOP` commands are deprecated and will be removed in the next release. They now
run `meds-extract-run spec=OMOP` (and print the command), translating the arguments below. Their output stays in
`$ROOT_OUTPUT_DIR/MEDS_cohort`; the pre-MEDS tables move from `$ROOT_OUTPUT_DIR/pre_MEDS` to
`$ROOT_OUTPUT_DIR/MEDS_cohort/omop_pre_meds`.

| `OMOP_MEDS` runner                                                                | `meds-extract-run spec=OMOP`                                  |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `raw_input_dir=RAW root_output_dir=ROOT`                                          | `output_dir=ROOT/MEDS_cohort do_download=false input_dir=RAW` |
| `do_download=True do_demo=True`                                                   | `dataset_key=demo` (data in `download_dest_dir=`)             |
| `DATASET_NAME=...` (environment)                                                  | `"overrides=['etl_metadata.dataset_name=...']"`               |
| `RAW_DATASET_VERSION=...` (environment)                                           | `dataset_version=...`                                         |
| `++prefer_source=True`, `++limit_subjects=50`, `athena_csv_dir=...`, `pre_meds_*` | `"overrides=['++stage_cfg.prefer_source=true']"`, ...         |
| `do_enrich=False`                                                                 | `'skip_stages=[sumeds_enrich]'`                               |
| `'sumeds_args=[--strict]'`, `'sumeds_args=[--offline]'`                           | `"overrides=['++stage_cfg.strict=true']"`, `...offline=true`  |
| `N_WORKERS=8` (environment)                                                       | `stage_runner_fp=runner.yaml` (see below)                     |
| `N_SUBJECTS_PER_SHARD=100000` (environment)                                       | `"overrides=['++stage_cfg.n_subjects_per_shard=100000']"`     |
| `do_overwrite=True`                                                               | a new `output_dir`                                            |

`OMOP_VERSION` is read as before. The recorded `dataset_version` no longer ends in `:OMOP_<version>`.

## OMOP CDM versions

`OMOP_VERSION` (5.3, 5.4 or 5.5; a patch release such as `5.4.2` counts as its minor version) picks the CDM
tables and their schemas from `omop-schema`, and the pre-MEDS table config
`src/OMOP_MEDS/configs/pre_MEDS/omop_<version>.yaml`. Each version's file is complete on its own: it lists every
table the ETL converts and the columns kept from it, under that version's CDM field names. Settings that do not
depend on the version (subject and admission ID columns, input file types, metadata columns to drop, NLP
features) are in `configs/pre_MEDS/common.yaml`.

A configured column missing from the input is skipped without a warning, so a misspelled column silently drops
data. `tests/test_omop_versions.py` therefore checks every configured column against the `omop-schema` schemas,
which that package tests against the official CDM field lists. It also requires tables the CDM left unchanged
between versions to be configured identically, so a fix made in one version's file is made in the others.

OMOP CDM 5.5 ([changes from 5.4](https://ohdsi.github.io/CommonDataModel/cdm55Changes.html)) only adds optional
fields and vocabulary-metadata tables, so a 5.4 export also converts as 5.5, to the same events. The pre-MEDS
output keeps the new `observation.value_as_date`, `observation.value_as_source_concept_id`,
`observation.unit_source_concept_id` and `specimen.visit_occurrence_id` / `visit_detail_id`. The event config is
shared by all versions, so no MEDS event field reads them yet. The new `pack_content`, `concept_metadata` and
`concept_relationship_metadata` tables are not converted.

### Upgrading from a single pre-MEDS config

`configs/pre_MEDS_minimal.yaml`, with its `5.3:` / `5.4:` blocks, and the unused `configs/pre_MEDS.yaml` were
replaced by one file per version. If you changed `pre_MEDS_minimal.yaml`, copy each table block into
`omop_<version>.yaml` without the version level, and the top-level settings into `common.yaml`. In Python,
`OMOP_MEDS.PRE_MEDS_CFG` became `OMOP_MEDS.omop_versions.pre_meds_config_path(version)`, and
`OMOP_MEDS.premeds_cfg` now holds only the shared settings.

The table lists in `configs/OMOP.yaml` (`OMOP_MEDS.OMOP_CFG`, `OMOP_MEDS.omop_cfg`) were replaced by the schemas'
`get_table_names()`, and `OMOP_MEDS.omop_versions.OMOPSchemaV55` by `omop_schema.schema.v5_5.OMOPSchemaV55`.

The OMOP 5.4 `visit_occurrence` and `visit_detail` blocks asked for `admitted_source_value` and
`admitted_source_concept_id` instead of the CDM 5.4 `admitted_from_source_value` and `admitted_from_concept_id`,
so pre-MEDS output from OMOP 5.4 data lacked them. They are now kept; no event reads them, so the MEDS events are
unchanged.

## MEDS-Extract settings

If you want to convert a large dataset, you can use parallelization with MEDS-transforms
(the MEDS-transformation step that takes the longest).

Using local parallelization with the `hydra-joblib-launcher` package (`OMOP_MEDS[local_parallelism]`), pass a
stage runner file that sets the number of workers:

```bash
printf 'parallelize:\n  n_workers: 8\n  launcher: joblib\n' > runner.yaml
meds-extract-run spec=OMOP output_dir=$MEDS_DIR do_download=false input_dir=$OMOP_DIR stage_runner_fp=runner.yaml
```

Moreover, you can set the number of subjects per shard (default 10,000) to balance the parallelization overhead
based on how many subjects you have in your dataset:

```bash
meds-extract-run spec=OMOP ... "overrides=['++stage_cfg.n_subjects_per_shard=100000']"
```

The first MEDS-Extract stage, `convert_to_parquet`, rewrites each pre-MEDS parquet table once, keeping only the
columns the event config reads. It replaced `shard_events`, whose `row_chunksize` / `infer_schema_length` settings
no longer exist; per-stage memory is bounded by the number of subjects per shard instead.

MEDS-Transforms starts every stage as `MEDS_transform-stage`, found on `PATH`: run `meds-extract-run` from the
activated environment this package is installed in (or with `uv run`).

### Upgrading from MEDS-Extract 0.6

MEDS-Extract 0.7 changed the pipeline and event (MESSY) config formats. The configs shipped here are already
ported, and the output events are unchanged on the demo and Synthea test datasets. If you maintain a modified
`event_configs.yaml` or `ETL.yaml`, port it the same way:

- `subject_id_col: person_id` becomes `_defaults: {subject_id: $person_id}`.
- In `ETL.yaml`, `event_conversion_config_fp` becomes `MESSY_config_fp`, `shard_events` becomes
    `convert_to_parquet`, and `extract_code_metadata` must come before `merge_to_MEDS_cohort`.
- Pre-MEDS tables written in several batches (large `measurement` / `observation` / `note` tables) are now a
    `pre_MEDS/<table>/` directory of parts instead of `pre_MEDS/<table>.parquet/`, the layout MEDS-Extract 0.7+
    reads. Older pre-MEDS output in the old layout is renamed automatically when the ETL resumes.
- A null component in an f-string code now drops the event instead of rendering as `UNK`. Coalesce every
    component to keep the old behaviour, e.g. `code: f"{$preferred_vocabulary_name ?? 'UNK'}//{$preferred_concept_name ?? 'UNK'}"`.
    Dropped rows are counted only in the stage logs under `$MEDS_DIR/convert_to_MEDS_events/.logs`.

## The MIMIC-IV OMOP Dataset

We use the demo dataset for MIMIC-IV in the OMOP format, which is a subset of the MIMIC-IV dataset.
This dataset downloaded from Physionet does not include the standard dictionary linking definitions but should otherwise
be functional. We also include a small sample of synthetic (LLM-generated) data in the test/demo_resources directory, which can be used for testing and development purposes.

## Code metadata (`codes.parquet`)

`metadata/codes.parquet` lists every code in the data, once per OMOP table it comes from (`table_name`,
`occurrence_count`), and links it to the OMOP concept it was built from (`concept_id`). Codes from tables
with a mapped and a source concept (observation, measurement, visit occurrence, condition and procedure
occurrences, and device exposure end) end in the concept's `concept_code`, e.g. `LOINC//718-7`; the other
tables end in its `concept_id`. From the source database's own vocabulary tables, each code gets:

- `description`: the `concept_name` in the `concept` table;
- `parent_codes`: the `vocabulary_id//concept_code` of every concept it "Maps to" in `concept_relationship`.
    A standard concept maps to itself, so a code ending in its `concept_id` gets its own vocabulary code
    (e.g. `LOINC//718-7`), while a code that already is that vocabulary code does not list itself;
- `vocabulary_id` and `concept_code` of the concept.

The file validates against `meds.CodeMetadataSchema`.

Concepts missing from the source's `concept` table (many OMOP extracts, including the MIMIC-IV demo, ship only a
subset) keep their `concept_id` but get no description. To fill those gaps, point the ETL at a local
[OHDSI Athena](https://athena.ohdsi.org) vocabulary download (the directory with `CONCEPT.csv` and
`CONCEPT_RELATIONSHIP.csv`):

```bash
meds-extract-run spec=OMOP output_dir=$MEDS_DIR do_download=false input_dir=$OMOP_DIR \
	"overrides=['++stage_cfg.athena_csv_dir=/path/to/athena']"
```

(`athena_csv_dir` is an option of the `omop_codes_metadata` stage, which writes `codes.parquet`.)

This only fills what is missing; values from the source tables are never changed. Each code is looked up by its
`concept_id`: descriptions come from Athena through [suMEDS](https://github.com/rvandewater/SumMEDS) (valid
concepts only), parent codes from the "Maps to" relationships of the source, else of Athena. On the MIMIC-IV demo,
this describes 7,987 of its 8,480 codes, which the source tables leave undescribed. You bring your own Athena
download: the ETL never ships vocabulary content, and CPT4 names are only in an Athena export completed with a UMLS
license (Athena's CPT4 utility and your UMLS API key).

### Enriching code metadata with suMEDS

Vocabulary ancestors come from [suMEDS](https://github.com/rvandewater/SumMEDS) rather than from this ETL. The last
stage of the `OMOP` spec, suMEDS's `sumeds_enrich`, enriches `metadata/codes.parquet` in place with the rules this
package ships, [`src/OMOP_MEDS/sumeds_rules.yaml`](src/OMOP_MEDS/sumeds_rules.yaml). They are registered in
suMEDS's `sumeds.rules` entry-point group as `OMOP`, so `--rules OMOP` works for any suMEDS command. The file holds
no rules: OMOP codes already are `vocabulary_id//concept_code`, which suMEDS reads without one. suMEDS replaces
`codes.parquet` only once the enriched table is complete, and it stays valid MEDS code metadata. The vocabulary
data comes from suMEDS's ontology packs (versioned, pinned release: ICD-10-CM, ICD-10-PCS, ICD-9-CM, ICD-9
procedures, LOINC), fetched from GitHub on first use and cached; no Athena download is needed. See
[Enrichment, and sites without internet](#enrichment-and-sites-without-internet) for turning it off, retrying it
and running it offline. To also add the LOINC Parts of the LOINC multiaxial hierarchy as parents of LOINC codes:

```bash
meds-extract-run spec=OMOP ... "overrides=['++stage_cfg.loinc_hierarchy=true']"
```

suMEDS validates every code that ends in a `concept_code` of a pack vocabulary. It adds the official title where
the code has none, and the code's vocabulary ancestors to `parent_codes`, after the "Maps to" parents from the ETL.
It also looks up those parents and appends their ancestors. Every added parent is named in `parent_names`, a map
from code to name. Nothing the ETL wrote is changed. A code ending in a `concept_id` is never matched itself, as no
concept id of these vocabularies looks like one of their codes. For OMOP data this mostly means the ancestors of the ICD-10-PCS and ICD-9 procedure codes of
`procedure_occurrence`; conditions, drugs and measurements are mostly SNOMED, RxNorm and LOINC, which the source
`concept` table already describes. On a credentialed OMOP CDM 5.4 extract, 4,396 of its 56,992 codes (3,682
ICD-10-PCS and 714 ICD-9 procedure codes) gained 23,491 parents. Synthea27Nj and the MIMIC-IV demo have no such
codes, so the packs add nothing to them, except LOINC Parts to 139 Synthea27Nj codes with `--loinc-hierarchy`.

suMEDS's own Athena option (`athena_csv`) would add Athena's `CONCEPT_ANCESTOR` ancestors for every vocabulary,
but the spec does not set it, and it is not recommended: suMEDS matches code strings, so a code ending in a `concept_id` matches the
concept whose `concept_code` is that number, a different concept. In Synthea27Nj, `RxNorm//1154062//start`
(concept 1154062, a fentanyl patch) matches RxNorm code 1154062 (an aspirin / butalbital product) and gets its
ancestors. `athena_csv_dir` (above) looks codes up by `concept_id` instead.

## Advanced datetime resolution

To handle datetime resolution in a more flexible way, you can configure the `datetime_resolver` settings in your config file.
This allows you to specify which columns to use for determining the datetime of events, with support for both primary and override columns.
The primary purpose of this is to prevent information leakage by using the last edit datetime (of notes) instead of the original note datetime, if available.
The following block is the `note` table's in each `configs/pre_MEDS/omop_<version>.yaml`, and you can adjust the
column names based on your dataset's schema:

```yaml
datetime_resolver:
  primary_datetime_col: note_datetime     # preferred; full timestamp
  primary_date_col: note_date     # fallback; promoted to 23:59:59
  override_datetime_col: xtn_note_last_edit_datetime     # optional XTN column
  override_date_col: xtn_note_last_edit_date     # optional XTN column
```

The following is an example of how the data then looks like:

| note_date  | note_datetime       | xtn_note_last_edit_date | xtn_note_last_edit_datetime |
| ---------- | ------------------- | ----------------------- | --------------------------- |
| 2023-01-01 | 2023-01-01 08:00:00 | 2023-01-01              | 2023-01-01 08:05:00         |
| 2023-01-02 | 2023-01-02 09:30:00 | 2023-01-02              | 2023-01-02 10:00:00         |

In this case it will prefer the `xtn_note_last_edit_datetime` column for the datetime of the event (if it is in fact later in time),
which can help to prevent information leakage in some cases, but it will fall back to `note_datetime`/`note_date` if the override column is not available.
See `build_preferred_event_datetime` function in the code for more details on how this works.

## Particularities

- Care site is added to the visit as text
- Add support for care_site table (visit_detail)

## Citation

If you use this ETL for your research, please use the citation link in Github, which points to the Zenodo DOI,
and which is also included below:

```
@software{van_de_Water_OMOP_MEDS_ETL_2025,
author = {van de Water, Robin Philippus},
doi = {10.5281/zenodo.15132444},
license = {MIT},
month = feb,
title = {{OMOP\_MEDS ETL}},
url = {https://github.com/rvandewater/OMOP_MEDS},
year = {2025}
}
```
