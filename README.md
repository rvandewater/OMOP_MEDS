# MEDS OMOP ETL

[![PyPI - Version](https://img.shields.io/pypi/v/OMOP_MEDS)](https://pypi.org/project/OMOP_MEDS/)
[![codecov](https://codecov.io/gh/rvandewater/OMOP_MEDS/graph/badge.svg?token=RW6JXHNT0W)](https://codecov.io/gh/rvandewater/OMOP_MEDS)
[![tests](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.11-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/OMOP_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/OMOP_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/OMOP_MEDS.svg)](https://github.com/rvandewater/OMOP_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/940565218.svg)](https://doi.org/10.5281/zenodo.15132443)
![Static Badge](https://img.shields.io/badge/MEDS-0.3.3-blue)

An ETL pipeline for transforming Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM)
datasets (into the MEDS format using the MEDS-Transforms library.
We gratefully acknowledge the developers of the first OMOP MEDS ETL, from which we took inspiration,
which can be found here: https://github.com/Medical-Event-Data-Standard/meds_etl.

We currently support OMOP 5.3 and 5.4 datasets. Earlier versions might work but are not tested and are perhaps (?)
not used in practice anymore. Please open pull requests if you want to add support for earlier versions.

- More information about OMOP can be found here: https://ohdsi.github.io/CommonDataModel/
- More information about MEDS can be found here: https://medical-event-data-standard.github.io/

## Setup

First install the package:

```bash
pip install OMOP_MEDS
```

Then:

```bash
export DATASET_NAME="Your_OMOP_Dataset_Name" # e.g. MIMIC_IV_OMOP
export OMOP_VERSION="5.3" # or 5.4
export RAW_INPUT_DIR="path/to/your/input"
export ROOT_OUTPUT_DIR="/path/to/your/output"
OMOP_MEDS raw_input_dir=$RAW_INPUT_DIR root_output_dir=$ROOT_OUTPUT_DIR
```

To try with the MIMIC-IV OMOP demo dataset (this downloads a version to your local machine), you can run:

```bash
OMOP_MEDS raw_input_dir=path/to/your/input root_output_dir=/path/to/your/output do_download=True ++do_demo=True
```

Example config for an OMOP dataset:

```yaml
dataset_name: MIMIC_IV_OMOP
raw_dataset_version: 1.0
omop_version: 5.3

urls:
  dataset:
    - https://physionet.org/content/mimic-iv-demo-omop/0.9/
    - url: EXAMPLE_CONTROLLED_URL
      username: ${oc.env:DATASET_DOWNLOAD_USERNAME}
      password: ${oc.env:DATASET_DOWNLOAD_PASSWORD}
  demo:
    - https://physionet.org/content/mimic-iv-demo-omop/0.9/
  common:
    - EXAMPLE_SHARED_URL # Often used for shared metadata files
```

Run this with:

```bash
OMOP_MEDS ++DATASET_CFG=your_config.yaml raw_input_dir=path/to/your/input root_output_dir=/path/to/your/output \
do_download=True
```

## Differences with the original meds_etl_omop

This package is designed as a more flexible and configurable alternative to the original `meds_etl_omop` package.
We make a few important choices that have impact on your downstream training and task definitions:

- We use the mapped concepts by default, which are more standardized across datasets and, for large, health systems can
    be more clean, especially if you are working with a limited tokenizer on a large dataset.
    You can still use the source concepts by setting `++prefer_source=True`.
- We use more tables than in the original `meds_etl_omop` package, which can lead to more complete patient histories.
    Watch for potential information leakage. You can change your table configs in pre_MEDS.yaml and event_configs.yaml
- This package is more resource intensive, please adjust your `n_shards` and watch your memory usage.

## Pre-MEDS settings

The following settings can be used to configure the pre-MEDS steps.

```bash
OMOP_MEDS \
	root_output_dir=/sc/arion/projects/hpims-hpi/projects/foundation_models_ehr/cohorts/meds_debug/small_demo \
	raw_input_dir=/sc/arion/projects/hpims-hpi/projects/foundation_models_ehr/cohorts/full_omop \
	do_download=False ++do_overwrite=True ++limit_subjects=50
```

- `root_output_dir`: Set the root output directory.
- `raw_input_dir`: Path to the raw input directory.
- `do_download`: Set to `False` to skip downloading the dataset.
- `++do_overwrite`: Set to `True` to overwrite existing files.
- `++limit_subjects`: Limit the number of subjects to process.

## MEDS-transforms settings

If you want to convert a large dataset, you can use parallelization with MEDS-transforms
(the MEDS-transformation step that takes the longest).

Using local parallelization with the `hydra-joblib-launcher` package, you can set the number of workers:

```
pip install hydra-joblib-launcher --upgrade
```

Then, you can set the number of workers as environment variable:

```bash
export N_WORKERS=8
```

Moreover, you can set the number of subjects per shard to balance the parallelization overhead based on how many
subjects you have in your dataset:

```bash
export N_SUBJECTS_PER_SHARD=100000
```

## The MIMIC-IV OMOP Dataset

We use the demo dataset for MIMIC-IV in the OMOP format, which is a subset of the MIMIC-IV dataset.
This dataset downloaded from Physionet does not include the standard dictionary linking definitions but should otherwise
be functional

## Particularities

- Care site is added to the visit as text
- Add support for care_site table (visit_detail)

## Citation

If you use this dataset, please use the citation link in Github.
