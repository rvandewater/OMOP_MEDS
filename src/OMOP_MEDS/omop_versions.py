"""The OMOP CDM versions this ETL reads: their pre-MEDS table configs and their table schemas.

Every supported version has its own pre-MEDS table config, `configs/pre_MEDS/omop_<version>.yaml`, chosen by the
`OMOP_VERSION` environment variable (see `dataset.yaml`). Settings shared by all versions are in
`configs/pre_MEDS/common.yaml`.
"""

import re

import pyarrow as pa
from omegaconf import DictConfig, OmegaConf
from omop_schema.schema.base import OMOPSchemaBase
from omop_schema.schema.v5_4 import OMOPSchemaV54
from omop_schema.utils import get_schema_loader as get_omop_schema_loader

from . import PRE_MEDS_CFG_DIR

SUPPORTED_OMOP_VERSIONS = ("5.3", "5.4", "5.5")
TABLE_BLOCK_KEYS = frozenset(
    {"reference_cols", "output_data_cols", "concept_cols", "datetime_resolver"}
)

_VERSION_PATTERN = re.compile(r"v?(\d+)\.(\d+)(?:\.\d+)?")


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
        ValueError: Unsupported OMOP CDM version '6.0'. Set OMOP_VERSION to one of: 5.3, 5.4, 5.5.
    """
    match = _VERSION_PATTERN.fullmatch(str(version).strip())
    normalized = f"{match[1]}.{match[2]}" if match else None
    if normalized not in SUPPORTED_OMOP_VERSIONS:
        raise ValueError(
            f"Unsupported OMOP CDM version {str(version)!r}. "
            f"Set OMOP_VERSION to one of: {', '.join(SUPPORTED_OMOP_VERSIONS)}."
        )
    return normalized


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


class OMOPSchemaV55(OMOPSchemaV54):
    """OMOP CDM 5.5: CDM 5.4 plus the fields and tables in https://ohdsi.github.io/CommonDataModel/cdm55Changes.html.

    CDM 5.5 only adds optional fields and three vocabulary-metadata tables; nothing is renamed or removed.
    `omop_schema` (0.0.4) has no 5.5 schema yet.

    Examples:
        >>> schema = OMOPSchemaV55()
        >>> schema.get_schema("observation")["value_as_date"], schema.get_schema("pack_content")["box_size"]
        (DataType(date64[ms]), DataType(int64))
    """

    def _load_schema(self) -> dict[str, dict[str, pa.DataType]]:
        schemas = super()._load_schema()
        added_fields = {
            "measurement": {"value_as_source_concept_id": pa.int64()},
            "observation": {
                "value_as_date": pa.date64(),
                "unit_source_concept_id": pa.int64(),
                "value_as_source_concept_id": pa.int64(),
            },
            "specimen": {
                "visit_occurrence_id": pa.int64(),
                "visit_detail_id": pa.int64(),
            },
            "cdm_source": {"cdm_release_identifier": pa.string()},
        }
        for table, fields in added_fields.items():
            schemas[table] = {**schemas[table], **fields}
        schemas["pack_content"] = {
            "pack_concept_id": pa.int64(),
            "drug_concept_id": pa.int64(),
            "amount": pa.int64(),
            "box_size": pa.int64(),
        }
        schemas["concept_metadata"] = {
            "concept_id": pa.int64(),
            "concept_category": pa.string(),
            "reuse_status": pa.string(),
        }
        schemas["concept_relationship_metadata"] = {
            "concept_id_1": pa.int64(),
            "concept_id_2": pa.int64(),
            "relationship_id": pa.string(),
            "relationship_predicate_id": pa.string(),
            "relationship_group": pa.int64(),
            "mapping_source": pa.string(),
            "confidence": pa.float64(),
            "mapping_tool": pa.string(),
            "mapper": pa.string(),
            "reviewer": pa.string(),
        }
        return schemas


def get_schema_loader(version: str | float) -> OMOPSchemaBase:
    """Return the table schemas for an OMOP CDM version.

    Examples:
        >>> type(get_schema_loader(5.3)).__name__, type(get_schema_loader("5.5")).__name__
        ('OMOPSchemaV53', 'OMOPSchemaV55')
    """
    version = normalize_omop_version(version)
    return OMOPSchemaV55() if version == "5.5" else get_omop_schema_loader(version)
