# MEDS OMOP ETL with MEDS-Transforms

[![PyPI - Version](https://img.shields.io/pypi/v/OMOP_MEDS)](https://pypi.org/project/OMOP_MEDS/)
[![codecov](https://codecov.io/gh/rvandewater/OMOP_MEDS/graph/badge.svg?token=RW6JXHNT0W)](https://codecov.io/gh/rvandewater/OMOP_MEDS)
[![tests](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/OMOP_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.11-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/OMOP_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/OMOP_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/OMOP_MEDS.svg)](https://github.com/rvandewater/OMOP_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/940565218.svg)](https://doi.org/10.5281/zenodo.15132443)

An ETL pipeline for transforming OMOP datasets into the MEDS format using the MEDS-Transforms library.
Thanks to the developers of the first OMOP MEDS ETL, from which we took inspiration,
which can be found here: https://github.com/Medical-Event-Data-Standard/meds_etl.
We currently support OMOP 5.3 and 5.4 datasets.

```bash
pip install OMOP_MEDS
OMOP_MEDS root_output_dir=$ROOT_OUTPUT_DIR
```

To try with the MIMIC-IV OMOP demo dataset, you can run:

```bash
OMOP_MEDS root_output_dir=/path/to/your/output do_download=True ++do_demo=True
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

## MEDS-transforms settings

If you want to convert a large dataset, you can use parallelization with MEDS-transforms
(the MEDS-transformation step that takes the longest).

Using local parallelization with the `hydra-joblib-launcher` package, you can set the number of workers:

```
pip install hydra-joblib-launcher --upgrade
```

Then, you can set the number of workers as environment variable:

```bash
export N_WORKERS=16
```

Moreover, you can set the number of subjects per shard to balance the parallelization overhead based on how many subjects you have in your dataset:

```
export N_SUBJECTS_PER_SHARD=1000
```

## Citation

If you use this dataset, please use the citation link in Github.
