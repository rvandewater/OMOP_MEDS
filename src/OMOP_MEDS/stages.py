"""The OMOP-specific steps of the ETL, as MEDS-Extract extension stages.

The registered spec `OMOP` (`configs/event_configs.yaml`) runs the whole ETL with one command,
`meds-extract-run spec=OMOP output_dir=OUT do_download=false input_dir=OMOP_DIR`:

1. `omop_pre_meds` (a pre-stage) reads the raw OMOP CDM tables in `input_dir` and writes the pre-MEDS tables
   (concepts joined, times resolved) into `OUT/omop_pre_meds`, where MEDS-Extract reads them instead of the
   raw tables;
2. MEDS-Extract's canonical stages extract the MEDS events;
3. `omop_codes_metadata` (a post-stage) writes `metadata/codes.parquet`: every code with its OMOP concept's
   description and "Maps to" parents, from the source database's own vocabulary tables;
4. `sumeds_enrich` (registered by suMEDS) adds vocabulary titles and ancestors, and records the enrichment in
   `metadata/dataset.json`.

Every stage process of a pipeline imports this module, so its top-level imports stay light: the pre-MEDS and
code metadata code is imported when a stage runs.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from MEDS_transforms.stages import Stage
from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)

PRE_MEDS_STAGE = "omop_pre_meds"
CODES_METADATA_STAGE = "omop_codes_metadata"
#: Written by `omop_pre_meds` into its output dir: the OMOP_MEDS version and the options that built the pre-MEDS
#: tables, which `omop_codes_metadata` records in `metadata/dataset.json`.
SETTINGS_FILE = "omop_meds_settings.json"

#: Options of `omop_pre_meds`. Set them in the spec (`- omop_pre_meds: {prefer_source: true}`) or for one run
#: with `"overrides=['++stage_cfg.prefer_source=true']"`.
PRE_MEDS_DEFAULTS: dict[str, Any] = {
    # The OMOP CDM version of the input (5.3, 5.4 or 5.5); null reads the OMOP_VERSION environment variable,
    # else 5.3.
    "omop_version": None,
    # Use the source concept rather than the mapped (standard) concept of tables that have both.
    "prefer_source": False,
    # Keep only persons with at least one visit.
    "join_on_visit": False,
    # Convert only the first N persons (0: all), for debugging.
    "limit_subjects": 0,
    # Tables that are not converted, without a warning.
    "tables_to_ignore": [
        "note_nlp",
        "fact_relationship",
        "location",
        "care_site",
        "provider",
        "payer_plan_period",
        "cost",
        "metadata",
        "cdm_source",
        "concept_class",
        "cohort_definition",
        "attribute_definition",
        "vocabulary",
        "concept_ancestor",
        "concept_relationship",
        "pack_content",
        "concept_metadata",
        "concept_relationship_metadata",
    ],
    # Batched pre-MEDS processing of very large tables (see the README).
    "pre_meds_chunked_tables": ["measurement", "observation", "note"],
    "pre_meds_batching_row_threshold": 10_000_000,
    "pre_meds_batch_mode": "auto",
    "pre_meds_batch_size_shards": 1,
    "pre_meds_batch_input_rows": 10_000_000,
}

#: Options of `omop_codes_metadata`.
CODES_METADATA_DEFAULTS: dict[str, Any] = {
    # A local OHDSI Athena vocabulary download (the directory with CONCEPT.csv and CONCEPT_RELATIONSHIP.csv),
    # to fill descriptions and parents of concepts missing from the source's own concept tables.
    "athena_csv_dir": None,
}


def pre_meds_config(
    raw_input_dir: Path | str, pre_MEDS_dir: Path | str, **options: Any
) -> DictConfig:
    """The configuration `pre_meds.main` reads: the `omop_pre_meds` defaults, `options` and the two dirs.

    Examples:
        >>> cfg = pre_meds_config("/raw", "/out/omop_pre_meds", limit_subjects=5)
        >>> cfg.raw_input_dir, cfg.pre_MEDS_dir, cfg.limit_subjects, cfg.prefer_source, cfg.do_overwrite
        ('/raw', '/out/omop_pre_meds', 5, False, False)
        >>> pre_meds_config("/raw", "/out", typo=1)
        Traceback (most recent call last):
            ...
        ValueError: Unknown pre-MEDS option(s) ['typo']; known: ...
    """
    known = {*PRE_MEDS_DEFAULTS, "do_overwrite", "required_tables"}
    if unknown := sorted(set(options) - known):
        raise ValueError(
            f"Unknown pre-MEDS option(s) {unknown}; known: {sorted(known)}"
        )
    cfg = {"do_overwrite": False, "required_tables": [], **PRE_MEDS_DEFAULTS, **options}
    cfg["raw_input_dir"] = str(raw_input_dir)
    cfg["pre_MEDS_dir"] = str(pre_MEDS_dir)
    return OmegaConf.create(cfg)


def _pre_meds(cfg: DictConfig) -> None:
    """Convert the raw OMOP CDM tables in `input_dir` into pre-MEDS tables in the stage's output dir.

    It joins every event table to its concepts (`preferred_vocabulary_name`, `preferred_concept_name`,
    `preferred_concept_id`), resolves note times, links persons to their death and birth, and writes the
    vocabulary tables the code metadata stage reads (`concept`, `concept_relationship`, `codes`). Every table
    the spec extracts is written: a table the input lacks, or that has no rows, is written empty, so the one
    registered spec fits every OMOP dataset. See `PRE_MEDS_DEFAULTS` for the options.
    """
    if cfg.worker != 0:
        return
    from MEDS_extract.config import MessyConfig

    from .pre_meds import main as pre_meds_main

    stage_cfg = cfg.stage_cfg
    out_dir = Path(stage_cfg.output_dir)
    options = {k: stage_cfg.get(k, v) for k, v in PRE_MEDS_DEFAULTS.items()}
    for key in ("tables_to_ignore", "pre_meds_chunked_tables"):
        options[key] = list(options[key])
    required_tables = sorted(
        MessyConfig.load(cfg.MESSY_config_fp).needed_source_columns()
    )
    pre_meds_main(
        pre_meds_config(
            cfg.input_dir,
            out_dir,
            do_overwrite=bool(cfg.get("do_overwrite", False)),
            required_tables=required_tables,
            **options,
        )
    )
    _write_settings(out_dir, options)


def _write_settings(out_dir: Path, options: dict[str, Any]) -> None:
    """Write `SETTINGS_FILE`: the OMOP_MEDS version and every option but batching, with the resolved OMOP version.

    Batching (`pre_meds_*`) does not change the output; every other option, including ones added later, is
    recorded. The options hold no paths.
    """
    from . import __version__, dataset_info
    from .omop_versions import normalize_omop_version

    settings = {k: v for k, v in options.items() if not k.startswith("pre_meds_")}
    settings["omop_version"] = normalize_omop_version(
        settings.get("omop_version") or dataset_info.omop_version
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / SETTINGS_FILE).write_text(
        json.dumps(
            {"omop_meds_version": __version__, **settings},
            default=lambda v: OmegaConf.to_container(v, resolve=True),
        )
    )


def _codes_metadata(cfg: DictConfig) -> None:
    """Write `metadata/codes.parquet`: every code in the data with its OMOP concept's metadata.

    Each code is linked to the OMOP concept it was built from and gets that concept's `concept_name` as its
    description, and the `vocabulary_id//concept_code` of every concept it "Maps to" as parents, from the
    pre-MEDS vocabulary tables of `omop_pre_meds`. With `athena_csv_dir`, gaps are filled from a local Athena
    download. See `OMOP_MEDS.utils.finish_codes_metadata`.
    """
    if cfg.worker != 0:
        return
    from MEDS_extract.config import MessyConfig

    from .utils import enrich_codes_metadata_with_athena, finish_codes_metadata

    stage_cfg = cfg.stage_cfg
    cohort_dir = Path(stage_cfg.data_input_dir).parent
    pre_MEDS_dir = Path(cfg.output_dir) / PRE_MEDS_STAGE
    spec_fp = MessyConfig.load(cfg.MESSY_config_fp).source_fp
    codes_fp = finish_codes_metadata(cohort_dir, pre_MEDS_dir, spec_fp)
    athena_csv_dir = stage_cfg.get("athena_csv_dir")
    if athena_csv_dir and codes_fp is not None:
        logger.info("Filling code metadata gaps from the Athena vocabulary download.")
        enrich_codes_metadata_with_athena(
            codes_fp, pre_MEDS_dir, Path(athena_csv_dir).expanduser()
        )
    record_provenance(cohort_dir, pre_MEDS_dir, athena_fallback=bool(athena_csv_dir))


def record_provenance(
    cohort_dir: Path, pre_MEDS_dir: Path, athena_fallback: bool = False
) -> None:
    """Record in `metadata/dataset.json` which OMOP_MEDS version and settings built the dataset.

    MEDS-Extract names MEDS-Transforms as the ETL; this sets `etl_name`/`etl_version` to OMOP_MEDS and adds an
    `omop_meds` block: the settings `omop_pre_meds` wrote (`SETTINGS_FILE`), the MEDS-Extract and MEDS-Transforms
    versions, and whether the Athena fallback was used (never its path). Other keys are kept, and a re-run gives
    the same file.

    Examples:
        >>> import tempfile
        >>> from OMOP_MEDS import __version__
        >>> with tempfile.TemporaryDirectory() as d:
        ...     fp = Path(d, "metadata", "dataset.json")
        ...     fp.parent.mkdir()
        ...     _ = fp.write_text(json.dumps({"dataset_name": "X", "etl_name": "MEDS_transforms"}))
        ...     record_provenance(Path(d), Path(d, PRE_MEDS_STAGE))
        ...     dataset = json.loads(fp.read_text())
        >>> dataset["dataset_name"], dataset["etl_name"], dataset["etl_version"] == __version__
        ('X', 'OMOP_MEDS', True)
        >>> sorted(dataset["omop_meds"])
        ['athena_fallback', 'meds_extract_version', 'meds_transforms_version', 'omop_meds_version']
    """
    from importlib.metadata import version

    from meds import DatasetMetadataSchema, dataset_metadata_filepath

    from . import __version__

    dataset_fp = cohort_dir / dataset_metadata_filepath
    if not dataset_fp.is_file():
        logger.warning(
            f"No {dataset_fp}: the OMOP_MEDS version and settings are not recorded."
        )
        return
    settings_fp = pre_MEDS_dir / SETTINGS_FILE
    if settings_fp.is_file():
        record = json.loads(settings_fp.read_text())
    else:
        logger.warning(
            f"No {settings_fp} from {PRE_MEDS_STAGE}: recording the versions only."
        )
        record = {"omop_meds_version": __version__}
    record |= {
        "meds_extract_version": version("MEDS_extract"),
        "meds_transforms_version": version("MEDS_transforms"),
        "athena_fallback": athena_fallback,
    }
    dataset = json.loads(dataset_fp.read_text())
    dataset |= {
        "etl_name": "OMOP_MEDS",
        "etl_version": __version__,
        "omop_meds": record,
    }
    DatasetMetadataSchema.validate(dataset)
    tmp_fp = dataset_fp.with_name(f".{dataset_fp.name}.tmp")
    tmp_fp.write_text(json.dumps(dataset))
    os.replace(tmp_fp, dataset_fp)


pre_meds_stage = Stage.register(
    main_fn=_pre_meds,
    stage_name=PRE_MEDS_STAGE,
    is_metadata=False,
    default_config=PRE_MEDS_DEFAULTS,
)
codes_metadata_stage = Stage.register(
    main_fn=_codes_metadata,
    stage_name=CODES_METADATA_STAGE,
    is_metadata=True,
    default_config=CODES_METADATA_DEFAULTS,
)
