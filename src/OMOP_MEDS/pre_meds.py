"""Performs pre-MEDS data wrangling for INSERT DATASET NAME HERE."""

from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger
from MEDS_transforms.utils import get_shard_prefix, write_lazyframe
from omegaconf import DictConfig

from src.OMOP_MEDS.pre_meds_utils import (
    DATASET_NAME,
    extract_metadata,
    get_patient_link,
    join_concept_and_process_psuedotime,
    load_raw_file,
)

from . import dataset_info, omop_cfg, premeds_cfg

# Name of the dataset
# Column name for admission ID associated with this particular admission
# Column name for subject ID
# List of file extensions to be processed
DATA_FILE_EXTENSIONS = premeds_cfg.raw_data_extensions
# List of tables to be ignored during processing
IGNORE_TABLES = []


def main(cfg: DictConfig) -> None:
    """Performs pre-MEDS data wrangling for INSERT DATASET NAME HERE."""

    logger.info(f"Loading table preprocessors from {premeds_cfg}...")
    preprocessors = premeds_cfg  # OmegaConf.load(premeds_cfg)
    functions = {}
    omop_version = float(dataset_info.omop_version)
    supported_omop_versions = [5.3, 5.4]
    logger.info(f"Expecting OMOP version: {omop_version}")
    omop_cfg_version = omop_cfg[omop_version]
    input_dir = Path(cfg.raw_input_dir)
    MEDS_input_dir = Path(cfg.root_output_dir) / "pre_MEDS"
    MEDS_input_dir.mkdir(parents=True, exist_ok=True)

    done_fp = MEDS_input_dir / ".done"
    if done_fp.is_file() and not cfg.do_overwrite:
        logger.info(
            f"Pre-MEDS transformation already complete as {done_fp} exists and "
            f"do_overwrite={cfg.do_overwrite}. Returning."
        )
        exit(0)

    all_fps = []
    for table in omop_cfg_version["tables"]:
        # Check for .csv and .parquet files
        if table in IGNORE_TABLES:
            logger.info(f"Skipping {table} as it is in the ignore list.")
            continue
        csv_files = list(input_dir.glob(f"{table}.csv"))
        parquet_files = list(input_dir.glob(f"{table}.parquet"))
        directories = list(input_dir.glob(f"{table}"))

        if csv_files:
            all_fps.extend(csv_files)
        elif parquet_files:
            all_fps.extend(parquet_files)
        elif directories:
            all_fps.extend(directories)
        else:
            logger.warning(f"No files found for {table}")

    for table_name, preprocessor_cfg in preprocessors.items():
        if table_name not in ["subject_id", "admission_id", "raw_data_extensions"]:
            logger.info(f"  Adding preprocessor for {table_name}:\n{preprocessor_cfg}")
            if any(item in supported_omop_versions for item in preprocessor_cfg.keys()):
                if omop_version in preprocessor_cfg:
                    preprocessor_cfg = preprocessor_cfg[omop_version]
                else:
                    raise ValueError(f"OMOP version {omop_version} not supported for {table_name}.")
            functions[table_name] = join_concept_and_process_psuedotime(
                table_name=table_name, **preprocessor_cfg
            )

    unused_tables = {}
    person_out_fp = MEDS_input_dir / "person.parquet"
    concept_out_fp = MEDS_input_dir / "concept.parquet"
    concept_relationship_out_fp = MEDS_input_dir / "concept_relationship.parquet"

    if concept_out_fp.is_file():
        logger.info(f"Reloading processed concepts df from {str(concept_out_fp.resolve())}")
        concept_df = pl.read_parquet(concept_out_fp, use_pyarrow=True).lazy()
    else:
        logger.info("Processing concepts table first...")
        if (input_dir / "concept.csv").is_file():
            concept_df = load_raw_file(input_dir / "concept.csv")
        # For some reason this is the concept table in the downloaded data
        elif (input_dir / "2b_concept.csv").is_file():
            concept_df = load_raw_file(input_dir / "2b_concept.csv")
        write_lazyframe(concept_df, concept_out_fp)

    if person_out_fp.is_file():  # and visit_out_fp.is_file():
        logger.info(f"Reloading processed patient df from {str(person_out_fp.resolve())}")
        patient_df = pl.scan_parquet(person_out_fp)
        # visit_df = pl.scan_parquet(visit_out_fp)
    else:
        logger.info("Processing patient table...")

        person_df = load_raw_file(input_dir / "person.csv")
        death_df = load_raw_file(input_dir / "death.csv")
        # visit_df = load_raw_file(input_dir / "visit_occurrence.csv")

        # logger.info(f"Loading {str(admissions_fp.resolve())}...")
        # person_df = load_raw_file(admissions_fp)

        patient_df = get_patient_link(person_df=person_df, death_df=death_df)
        write_lazyframe(patient_df, person_out_fp)
        # write_lazyframe(visit_df, visit_out_fp)
    if concept_relationship_out_fp.is_file():
        logger.info(
            f"Reloading processed concept_relationship df from {str(concept_relationship_out_fp.resolve())}"
        )
        concept_relationship_df = pl.scan_parquet(concept_relationship_out_fp)
    else:
        logger.info("Processing concept_relationship table first...")
        concept_relationship_fp = input_dir / "concept_relationship.csv"
        logger.info(f"Loading {str(concept_relationship_fp.resolve())}...")
        concept_relationship_df = load_raw_file(concept_relationship_fp)
        write_lazyframe(concept_relationship_df, concept_relationship_out_fp)

    # patient_df = patient_df.join(visit_df, on=SUBJECT_ID)
    metadata = extract_metadata(concept_df, concept_relationship_df)
    metadata.sink_parquet(MEDS_input_dir / "codes.parquet")

    for in_fp in all_fps:
        pfx = get_shard_prefix(input_dir, in_fp)
        if pfx in unused_tables:
            logger.warning(f"Skipping {pfx} as it is not supported in this pipeline.")
            continue
        elif pfx not in functions:
            logger.warning(f"No function needed for {pfx}. For {DATASET_NAME}, THIS IS UNEXPECTED")
            continue

        out_fp = MEDS_input_dir / f"{pfx}.parquet"

        if out_fp.is_file():  # and not pfx == "data_float_h" :
            logger.info(f"Done with {pfx}. Continuing")
            continue

        out_fp.parent.mkdir(parents=True, exist_ok=True)

        st = datetime.now()
        logger.info(f"Processing {pfx}...")
        df = load_raw_file(in_fp)

        fn = functions[pfx]
        processed_df = fn(df, concept_df)

        # if "visit_occurrence_id" in schema.names():
        #     metadata["visit_id"] = pl.col("visit_occurrence_id").cast(pl.Int64)
        # unit_columns = []
        # if "unit_source_value" in schema.names():
        #     unit_columns.append(pl.col("unit_source_value"))
        # if "unit_concept_id" in schema.names():
        #     unit_columns.append(
        #         pl.col("unit_concept_id").replace_strict(concept_id_map,
        #         return_dtype=pl.Utf8(), default=None))
        # if unit_columns:
        #     metadata["unit"] = pl.coalesce(unit_columns)
        #
        # if "load_table_id" in schema.names():
        #     metadata["clarity_table"] = pl.col("load_table_id")
        #
        # if "note_id" in schema.names():
        #     metadata["note_id"] = pl.col("note_id")
        #
        # if (table_name + "_end_datetime") in schema.names():
        #     end = cast_to_datetime(schema, table_name + "_end_datetime", move_to_end_of_day=True)
        #     metadata["end"] = end
        logger.info(
            f"patient_df schema: {patient_df.collect_schema()}, "
            f"processed_df schema: {processed_df.collect_schema()}"
        )
        processed_df = processed_df.with_columns(table_name=pl.lit(pfx))
        processed_df.sink_parquet(out_fp)
        logger.info(f"Processed and wrote to {str(out_fp.resolve())} in {datetime.now() - st}")

    logger.info(f"Done! All dataframes processed and written to {str(MEDS_input_dir.resolve())}")
    return
