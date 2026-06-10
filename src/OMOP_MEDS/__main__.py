#!/usr/bin/env python
import logging
import os
import shutil
import sys
from pathlib import Path
from importlib.metadata import version

import hydra
from omegaconf import DictConfig, OmegaConf, omegaconf

from OMOP_MEDS.utils import finish_codes_metadata
from . import ETL_CFG, EVENT_CFG, MAIN_CFG, RUNNER_CFG
from . import __version__ as PKG_VERSION
from . import dataset_info
from .commands import run_command
from .download import download_data
from .pre_meds import main as pre_MEDS_transform
from .pre_meds_utils import rename_demo_files

logger = logging.getLogger(__name__)


@hydra.main(
    version_base=None, config_path=str(MAIN_CFG.parent), config_name=MAIN_CFG.stem
)
def main(cfg: DictConfig):
    """Runs the end-to-end MEDS Extraction pipeline."""
    raw_input_dir = Path(cfg.raw_input_dir)
    pre_MEDS_dir = Path(cfg.pre_MEDS_dir)
    MEDS_cohort_dir = Path(cfg.MEDS_cohort_dir)
    stage_runner_fp = cfg.get("stage_runner_fp", None)
    root_output_dir = Path(cfg.root_output_dir)
    if cfg.do_overwrite:
        if root_output_dir.exists():
            logger.info("Removing existing MEDS cohort directory.")
            shutil.rmtree(root_output_dir)
        if pre_MEDS_dir.exists():
            logger.info("Removing existing pre-MEDS directory.")
            shutil.rmtree(pre_MEDS_dir)
    else:
        logger.info("Not overwriting existing data as do_overwrite is False.")
    # Step 0: Data downloading
    if cfg.do_download:  # pragma: no cover
        if cfg.get("do_demo", False):
            logger.info("Downloading demo data.")
            download_data(raw_input_dir, dataset_info, do_demo=True)
            rename_demo_files(raw_input_dir)
        else:
            logger.info("Downloading data.")
            download_data(raw_input_dir, dataset_info)
            rename_demo_files(raw_input_dir)
    else:  # pragma: no cover
        logger.info("Skipping data download.")

    if cfg.do_demo:
        rename_demo_files(raw_input_dir)

    # Step 1: Pre-MEDS Data Wrangling
    event_cfg = OmegaConf.load(EVENT_CFG)
    event_cfg_new = event_cfg.copy()
    pre_MEDS_transform(cfg)
    pre_MEDS_dir_files = [item.stem for item in pre_MEDS_dir.iterdir()]
    for item in event_cfg.keys():
        if item not in pre_MEDS_dir_files and item != "subject_id_col":
            logger.warning(f"Removing table {item} from event config.")
            event_cfg_new.pop(item)

    if len(event_cfg_new.keys()) != len(event_cfg.keys()):
        event_cfg_path = pre_MEDS_dir / "event_configs.yaml"
        with open(event_cfg_path, "w") as f:
            omegaconf.OmegaConf.save(config=event_cfg_new, f=f)
    else:
        event_cfg_path = EVENT_CFG

    # Step 2: MEDS Cohort Creation
    # Ensure output directories exist before MEDS runs.
    # CI failures showed lockfile creation can error if parent directories don't exist.
    MEDS_cohort_dir.mkdir(parents=True, exist_ok=True)

    # Ensure the pre-meds dir exists (should, but defensive)
    pre_MEDS_dir.mkdir(parents=True, exist_ok=True)

    # First we need to set some environment variables
    command_parts = [
        f"DATASET_NAME={dataset_info.dataset_name}",
        f"DATASET_VERSION={dataset_info.raw_dataset_version}:{PKG_VERSION}:OMOP_{dataset_info.omop_version}",
        f"EVENT_CONVERSION_CONFIG_FP={str(Path(event_cfg_path).resolve())}",
        f"PRE_MEDS_DIR={str(pre_MEDS_dir.resolve())}",
        f"MEDS_COHORT_DIR={str(MEDS_cohort_dir.resolve())}",
    ]

    # Then we construct the rest of the command
    command_parts.extend(
        [
            "MEDS_transform-runner",
            f"--config-path={str(RUNNER_CFG.parent.resolve())}",
            f"--config-name={RUNNER_CFG.stem}",
            f"pipeline_config_fp={str(ETL_CFG.resolve())}",
        ]
    )
    # On CI (ubuntu), running in fully-serial mode can hit a race/ordering issue
    # in the upstream lockfile creation for shard_events outputs.
    # Avoid disabling parallelize unless explicitly requested.
    if (
        os.getenv("OMOP_MEDS_FORCE_SERIAL", "0") == "1"
        or int(os.getenv("N_WORKERS", 1)) <= 1
    ):
        # Keep parallelize config present but force 1 worker when N_WORKERS isn't set.
        os.environ.setdefault("N_WORKERS", "1")

    if stage_runner_fp:
        command_parts.append(f"stage_runner_fp={stage_runner_fp}")

    command_parts.append("'hydra.searchpath=[pkg://MEDS_transforms.configs]'")
    run_command(command_parts, cfg)

    # Copy codes.parquet to MEDS cohort directory
    finish_codes_metadata(MEDS_cohort_dir, pre_MEDS_dir)


if __name__ == "__main__":
    if "--version" in sys.argv:
        print(f"OMOP_MEDS version: {version('OMOP-MEDS')}")
        print(f"Hydra version: {hydra.__version__}")
        sys.exit(0)
    main()
