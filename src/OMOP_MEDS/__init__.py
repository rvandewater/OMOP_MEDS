from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

from omegaconf import OmegaConf

__package_name__ = "OMOP_MEDS"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

# The MESSY spec registered as `OMOP` (see pyproject.toml).
EVENT_CFG = files(__package_name__).joinpath("configs/event_configs.yaml")
# Pre-MEDS table configs, one per OMOP CDM version (see omop_versions.py), and the settings they share.
PRE_MEDS_CFG_DIR = files(__package_name__).joinpath("configs/pre_MEDS")
PRE_MEDS_COMMON_CFG = PRE_MEDS_CFG_DIR.joinpath("common.yaml")
DATASET_CFG = files(__package_name__).joinpath("dataset.yaml")

dataset_info = OmegaConf.load(DATASET_CFG)
premeds_cfg = OmegaConf.load(PRE_MEDS_COMMON_CFG)

event_config = OmegaConf.load(EVENT_CFG)

__all__ = [
    "event_config",
    "EVENT_CFG",
    "PRE_MEDS_CFG_DIR",
    "PRE_MEDS_COMMON_CFG",
    "DATASET_CFG",
    "dataset_info",
    "__package_name__",
    "__version__",
]
