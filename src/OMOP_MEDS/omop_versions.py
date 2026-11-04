"""The OMOP CDM versions this ETL reads, and their pre-MEDS table configs.

Every supported version has its own pre-MEDS table config, `configs/pre_MEDS/omop_<version>.yaml`, chosen by the
`OMOP_VERSION` environment variable (see `dataset.yaml`). Settings shared by all versions are in
`configs/pre_MEDS/common.yaml`. The table schemas, and which versions have one, come from `omop_schema`.
"""

from omegaconf import DictConfig, OmegaConf
from omop_schema.versions import SUPPORTED_VERSIONS, get_schema_loader
from omop_schema.versions import normalize_omop_version as _normalize_omop_version

from . import PRE_MEDS_CFG_DIR

__all__ = [
    "SUPPORTED_OMOP_VERSIONS",
    "TABLE_BLOCK_KEYS",
    "get_schema_loader",
    "load_pre_meds_tables",
    "normalize_omop_version",
    "pre_meds_config_path",
]

SUPPORTED_OMOP_VERSIONS = SUPPORTED_VERSIONS
TABLE_BLOCK_KEYS = frozenset(
    {"reference_cols", "output_data_cols", "concept_cols", "datetime_resolver"}
)


def normalize_omop_version(version: str | float) -> str:
    """Return the supported OMOP CDM version `version` names, as `"<major>.<minor>"`.

    Patch releases (e.g. CDM v5.4.2) share their minor version's tables and fields.

    Raises:
        ValueError: If `version` is not a supported OMOP CDM version.

    Examples:
        >>> normalize_omop_version(5.4), normalize_omop_version("v5.4.2"), normalize_omop_version(" 5.5 ")
        ('5.4', '5.4', '5.5')
        >>> normalize_omop_version("6.0")
        Traceback (most recent call last):
        ...
        ValueError: Unsupported OMOP CDM version '6.0'; supported versions: 5.3, 5.4, 5.5. Set OMOP_VERSION to one of them.
    """
    try:
        return _normalize_omop_version(version)
    except ValueError as e:
        raise ValueError(f"{e} Set OMOP_VERSION to one of them.") from e


def pre_meds_config_path(version: str | float):
    """Return the packaged pre-MEDS table config for an OMOP CDM version.

    Examples:
        >>> pre_meds_config_path("5.4").name
        'omop_5.4.yaml'
    """
    return PRE_MEDS_CFG_DIR.joinpath(f"omop_{normalize_omop_version(version)}.yaml")


def load_pre_meds_tables(version: str | float) -> DictConfig:
    """Load the pre-MEDS table blocks for an OMOP CDM version.

    Every call returns a new config, so a caller may pop keys (such as `datetime_resolver`) from it.

    Raises:
        ValueError: If the version is unsupported, or a block is not a mapping of the keys in
            `TABLE_BLOCK_KEYS`.

    Examples:
        >>> tables = load_pre_meds_tables("5.5")
        >>> list(tables.visit_occurrence.reference_cols)
        ['visit_concept_id', 'visit_source_concept_id']
        >>> "datetime_resolver" in tables.note, tables is load_pre_meds_tables("5.5")
        (True, False)
    """
    fp = pre_meds_config_path(version)
    tables = OmegaConf.load(fp)
    for table, block in tables.items():
        if not isinstance(block, DictConfig):
            problem = f"got {block!r}"
        elif unknown := sorted(map(str, set(block) - TABLE_BLOCK_KEYS)):
            problem = f"got unknown keys {unknown}"
        else:
            continue
        raise ValueError(
            f"{fp}: `{table}` must be a table block with keys from {sorted(TABLE_BLOCK_KEYS)}; {problem}. "
            "Shared settings belong in common.yaml."
        )
    return tables
