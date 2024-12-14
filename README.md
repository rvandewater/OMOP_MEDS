# Extract your custom dataset via MEDS-Transforms

[![codecov](https://codecov.io/gh/mmcdermott/ETL_MEDS_Template/graph/badge.svg?token=RW6JXHNT0W)](https://codecov.io/gh/mmcdermott/ETL_MEDS_Template)
[![tests](https://github.com/mmcdermott/ETL_MEDS_Template/actions/workflows/tests.yaml/badge.svg)](https://github.com/mmcdermott/ETL_MEDS_Template/actions/workflows/tests.yml)
[![code-quality](https://github.com/mmcdermott/ETL_MEDS_Template/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/mmcdermott/ETL_MEDS_Template/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.12-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/mmcdermott/ETL_MEDS_Template#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/mmcdermott/ETL_MEDS_Template/pulls)
[![contributors](https://img.shields.io/github/contributors/mmcdermott/ETL_MEDS_Template.svg)](https://github.com/mmcdermott/ETL_MEDS_Template/graphs/contributors)

A template repository for a MEDS-Transforms powered extraction pipeline for a custom dataset.

## How to use this repository

1. Initialize a new repository using this template repository.
2. Rename the directory after `src/` to the name of your package in python-friendly format (e.g.,
    `MIMIC_IV_MEDS`).
3. Customize the following code points:
    - [`pyproject.toml`](#pyprojecttoml)
    - [`src/.../dataset.yaml`](#datasetyaml)
    - [`src/.../pre_MEDS.py`](#pre_medspy)
    - [`src/.../event_configs.yaml`](#event_configsyaml)
    - [`README.md`](#readmemd)
4. Customize the following external services:
    - CodeCov
    - PyPI

### Code Points:

#### `pyproject.toml`

In the `pyproject.toml` file, you will need to update the following fields:

1. Under `[project]`:
    - `name = "ETL-MEDS"`: Update `ETL-MEDS` to the name of your package (e.g., `MIMIC-IV-MEDS`)
    - `authors = [...]`: Update the author information to your name and email.
    - `description = "..."`: Update the description to a brief description of your dataset.
    - `dependencies = [...]`: Update the dependencies to include the necessary packages for your ETL pipeline
        (if any additional packages are needed).
2. Under `[project.scripts]`
    - `MEDS_extract-sample_dataset = "ETL_MEDS.__main__:main"`: Update `MEDS_extract-sample_dataset` to the
        name of your command-line pipeline (e.g., `MIMIC-IV_extract`) and update `ETL_MEDS` to the name of your
        package that you would import in python (e.g., `MIMIC_IV_MEDS`). This will be the same as the directory
        name between `src` and your actual code.
3. Under `[project.urls]`
    - `Homepage = "..."` Update the homepage to the URL of your GitHub repository.
    - `Issues = "..."` Update the issues URL to the URL of your GitHub repository issues page.

#### `src/.../dataset.yaml`

In this file, you can add details about the dataset you are working with. This will be used to record metadata
about the dataset and to provide links from which the dataset can be downloaded. You'll need to modify:

1. `dataset_name`: The name of the dataset.
2. `raw_dataset_version`: The version this version of your pipeline is designed to work with.
3. `urls`: This block contains the URLs from which the dataset can be downloaded. This field requires
    additional commentary, explored below.

##### URLs:

This field is an object and contains three sub-keys:

1. `dataset`: The URLs for the full dataset.
2. `demo`: The URLs for a smaller, open, demo version of the dataset.
3. `common`: The URLs for shared metadata files or other shared resources.

Each of these sub-keys should be a list of either strings (plain URLs) or dictionaries containing the URL (in
the key `url`) and username and password authentication information (in the keys `username` and `password`).
Note that we _strongly_ recommend that you _do not_ include your username and password in the raw file.
Instead, leverage the OmegaConf resolvers to reference external environment variables or other secure methods
of storing this information. In the example in this repository, we include one URL with the following
configuration:

```yaml
  - url: EXAMPLE_CONTROLLED_URL
    username: ${oc.env:DATASET_DOWNLOAD_USERNAME}
    password: ${oc.env:DATASET_DOWNLOAD_PASSWORD}
```

which would resolve to fill in the `username` and `password` from the environment variables
`DATASET_DOWNLOAD_USERNAME` and `DATASET_DOWNLOAD_PASSWORD`, respectively.

#### `pre_MEDS.py`

This script should be generally modified to include any "pre-MEDS" steps that are necessary to prepare the
dataset for MEDS-Transforms based extraction. Critically, these steps often include:

1. De-compressing files or otherwise preparing the raw data for extraction at a technical level.
2. Joining tables together so that all relevant rows include the unifying `subject_id`.
3. Converting any offsets into timestamps.
4. Any other modifications of interest.

See [MEDS-Transforms](TODO) for more documentation on the appropriate construction of the pre-MEDS script.

#### `event_configs.yaml`

This file is the configuration file for mapping the rows in your various raw data tables to MEDS events via
the MEDS-Transforms pipeline. See [MEDS-Transforms](TODO) for more documentation on the format and usage of
this file.

#### `README.md`

Insert badges like below:

```markdown
[![PyPI - Version](https://img.shields.io/pypi/v/PACKAGE_NAME)](https://pypi.org/project/PACKAGE_NAME/)
[![Documentation Status](https://readthedocs.org/projects/REPO_NAME/badge/?version=latest)](https://REPO_NAME.readthedocs.io/en/stable/?badge=stable)
[![codecov](https://codecov.io/gh/mmcdermott/REPO_NAME/graph/badge.svg?token=REPO_TOKEN)](https://codecov.io/gh/mmcdermott/REPO_NAME)
[![tests](https://github.com/mmcdermott/REPO_NAME/actions/workflows/tests.yaml/badge.svg)](https://github.com/mmcdermott/REPO_NAME/actions/workflows/tests.yml)
[![code-quality](https://github.com/mmcdermott/REPO_NAME/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/mmcdermott/REPO_NAME/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.12-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/mmcdermott/REPO_NAME#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/mmcdermott/REPO_NAME/pulls)
[![contributors](https://img.shields.io/github/contributors/mmcdermott/REPO_NAME.svg)](https://github.com/mmcdermott/REPO_NAME/graphs/contributors)
```

### External Services

#### CodeCov

1. Go to [CodeCov](https://codecov.io/) and add make an account or log-in as needed.
2. Follow the instructions to configure your new repository with CodeCov.
3. Copy the badge markdown from CodeCov and paste it into the `README.md` file. To find the badge markdown
    link, go to your repository in CodeCov, click on the "Configuration" tab, click on the "Badges and
    Graphs" option, then copy the markdown link from the top section and paste it in the corresponding line
    of the README, in place of the default link included above.
4. It will now track the test coverage of your ETL, including running the full pipeline against the linked
    demo data you provide in `dataset.yaml`.

#### PyPI

1. Go to [PyPI](https://pypi.org/) and add make an account or log-in as needed.
2. Go to your account settings and go to the "Publishing" settings.
3. Set up a new "Trusted Publisher" for your GitHub Repository (e.g., see the image below). Ensure your
    package name matches in the trusted publisher and in your `pyproject.toml` file!
4. Now, if, on the local command line, you run `git tag 0.0.1`, then `git push origin 0.0.1`, it will push a
    new, tagged version of your code as of the local commit when you ran the command both to a new GitHub
    Release and to PyPI. This will allow you to install your package via `pip install PACKAGE_NAME` and to manage
    versions effectively!

Example trusted publisher set-up:
![PyPI Trusted Publisher](https://github.com/mmcdermott/ETL_MEDS_Template/blob/main/static/pypi_trusted_publisher_example.png?raw=true)
