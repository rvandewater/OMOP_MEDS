"""Performs pre-MEDS data wrangling for OMOP datasets."""

import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
import polars.selectors as cs

from loguru import logger
from MEDS_transforms.utils import get_shard_prefix
from omegaconf import DictConfig
from omop_schema.utils import get_schema_loader

from . import dataset_info, omop_cfg, premeds_cfg
from .pre_meds_utils import (
    DATASET_NAME,
    ShardedTableDataLoader,
    get_table_path,
    join_concept,
    col_selector,
    set_up_metadata,
)
from tqdm import tqdm

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
    schema_loader = get_schema_loader(omop_version)
    if cfg.prefer_source:
        logger.warning(
            "Preferring source values over mapped values when available (e.g., Epic over LOINC)."
            " This has major implications for downstream analysis."
        )
    OMOP_input_dir = Path(cfg.raw_input_dir)
    MEDS_input_dir = Path(cfg.root_output_dir) / "pre_MEDS"
    MEDS_input_dir.mkdir(parents=True, exist_ok=True)
    limit = cfg.get("limit_subjects", 0)
    if limit > 0:
        logger.info(f"Limiting to {limit} subjects for debugging purposes.")
    done_fp = MEDS_input_dir / ".done"
    if done_fp.is_file() and not cfg.do_overwrite:
        logger.info(
            f"Pre-MEDS transformation already complete as {done_fp} exists and "
            f"do_overwrite={cfg.do_overwrite}. Returning."
        )
        return
    elif cfg.do_overwrite:
        logger.info(
            f"do_overwrite=True, removing existing pre-MEDS directory at {MEDS_input_dir}"
        )
        shutil.rmtree(MEDS_input_dir, ignore_errors=True)
        MEDS_input_dir.mkdir(parents=True, exist_ok=True)
    else:
        if any(MEDS_input_dir.iterdir()):
            logger.warning(
                f"Partial run found at {MEDS_input_dir}; will not overwrite existing files"
            )
    all_fps = []
    for table in omop_cfg_version["tables"]:
        # Check for .csv and .parquet files
        if table in IGNORE_TABLES:
            logger.info(f"Skipping {table} as it is in the ignore list.")
            continue
        csv_files = list(OMOP_input_dir.glob(f"{table}.csv"))
        parquet_files = list(OMOP_input_dir.glob(f"{table}.parquet"))
        directories = list(OMOP_input_dir.glob(f"{table}"))

        if csv_files:
            all_fps.extend(csv_files)
        elif parquet_files:
            all_fps.extend(parquet_files)
        elif directories:
            all_fps.extend(directories)
        else:
            logger.warning(f"No files found for {table}")

    def add_preprocessor(
        table_name,
        preprocessor_cfg,
    ):
        logger.info(f"  Adding preprocessor for {table_name}:\n{preprocessor_cfg}")
        if any(item in supported_omop_versions for item in preprocessor_cfg.keys()):
            if omop_version in preprocessor_cfg:
                preprocessor_cfg = preprocessor_cfg[omop_version]
            else:
                raise ValueError(
                    f"OMOP version {omop_version} not supported for {table_name}."
                )
        functions[table_name] = join_concept(
            table_name=table_name,
            **preprocessor_cfg,
            prefer_source=cfg.prefer_source,
        )

    pl.Config.set_streaming_chunk_size(50_000)  # default is ~200k–1M; tune downward
    for table_name, preprocessor_cfg in preprocessors.items():
        if table_name in [
            "subject_id",
            "admission_id",
            "raw_data_extensions",
            "expected_not_processed_tables",
            "tables_to_ignore",
            "metadata_cols_to_drop",
        ]:
            # These are config variables and not tables
            continue
        out_fp = MEDS_input_dir
        if (MEDS_input_dir / f"{table_name}.parquet").is_file() or (
            MEDS_input_dir / (f"{table_name}.parquet")
        ).is_dir():
            output_file = (
                MEDS_input_dir / f"{table_name}.parquet"
                if (MEDS_input_dir / f"{table_name}.parquet").is_file()
                else MEDS_input_dir / table_name
            )
            if cfg.do_overwrite:
                # If the output file already exists and do_overwrite is True, remove it before adding the preprocessor
                logger.info(
                    f"{str(output_file.resolve())} already exists but do_overwrite=True, so removing and re-processing {table_name}."
                )
                if output_file.is_file():
                    output_file.unlink()
                elif output_file.is_dir():
                    shutil.rmtree(output_file)
                add_preprocessor(table_name, preprocessor_cfg)
            else:
                # If the output file already exists and do_overwrite is False, skip adding the preprocessor for this table
                logger.info(
                    f"{str(output_file.resolve())} already exists and do_overwrite=False, so skipping {table_name}."
                )
                continue
        else:
            # If the output file does not exist, add the preprocessor for this table
            add_preprocessor(table_name, preprocessor_cfg)

    if premeds_cfg.get("metadata_cols_to_drop", False):
        metadata_cols_to_drop = premeds_cfg.get(
            "metadata_cols_to_drop", {"columns": [], "patterns": []}
        )
        logger.info(metadata_cols_to_drop)
        selector = ~col_selector(
            columns=metadata_cols_to_drop.get("columns", []),
            patterns=metadata_cols_to_drop.get("patterns", []),
        )
    else:
        selector = cs.all()
    logger.info(selector)

    chunked_tables = list(cfg.get("pre_meds_chunked_tables", None))
    data_loader = ShardedTableDataLoader(
        schema_loader=schema_loader,
        selector=selector,
        chunked_tables=chunked_tables,
        batching_row_threshold=int(
            cfg.get("pre_meds_batching_row_threshold", 1_000_000)
        ),
        batch_mode=str(cfg.get("pre_meds_batch_mode", "by_rows")),
        batch_size_shards=int(cfg.get("pre_meds_batch_size_shards", 1)),
        batch_input_rows=int(cfg.get("pre_meds_batch_input_rows", 10_000_000)),
    )

    unused_tables = {}

    concept_df, patient_df = set_up_metadata(
        MEDS_input_dir=MEDS_input_dir,
        do_overwrite=cfg.do_overwrite,
        OMOP_input_dir=OMOP_input_dir,
        limit=limit,
        schema_loader=schema_loader,
        selector=selector,
        join_on_visit=cfg.join_on_visit,
    )

    # Cache care_site lookup once per run; False means unavailable and skip subsequent attempts.
    care_site_lookup: pl.LazyFrame | bool | None = None

    def maybe_join_visit_occurrence_care_site(
        table_name: str, table_df: pl.LazyFrame
    ) -> pl.LazyFrame:
        nonlocal care_site_lookup
        if table_name != "visit_occurrence":
            return table_df

        if care_site_lookup is False:
            return table_df

        if care_site_lookup is None:
            care_site_in_fp = get_table_path(OMOP_input_dir, "care_site")
            if not care_site_in_fp:
                logger.warning(
                    "No care_site table found in the input directory. Skipping join with care_site."
                )
                care_site_lookup = False
                return table_df

            loaded = data_loader.load_table(care_site_in_fp)
            if loaded is None:
                logger.warning(
                    "Could not read care_site table from input directory. Skipping join with care_site."
                )
                care_site_lookup = False
                return table_df

            care_site_lookup = loaded.select(["care_site_id", "care_site_name"])

        return table_df.join(care_site_lookup, on="care_site_id", how="left")

    # Main loop that processes all tables with defined preprocessors, skipping those without and logging appropriately.
    # Uses batched loading and processing for large tables to avoid memory issues.

    # Special tables are processed separately beforehand
    special_tables = ["person", "death", "concept"]
    for in_fp in all_fps:
        tbl_prefix = get_shard_prefix(OMOP_input_dir, in_fp)
        out_fp = MEDS_input_dir / f"{tbl_prefix}.parquet"

        if tbl_prefix in unused_tables:
            logger.warning(
                f"Skipping {tbl_prefix} as it is not supported in this pipeline."
            )
            continue
        elif tbl_prefix not in functions:
            if tbl_prefix in special_tables:
                logger.info(
                    f"Skipping {tbl_prefix} as it has already been processed separately."
                )
            elif tbl_prefix in cfg.get("tables_to_ignore", []):
                logger.warning(
                    f"{tbl_prefix} will not be processed; this is seen as expected."
                )
            else:
                logger.warning(
                    f"No function needed for {tbl_prefix}. For {DATASET_NAME}, "
                    f"THIS IS COULD BE UNEXPECTED if {tbl_prefix} is in your OMOP source file and configuration."
                )
            continue

        if out_fp.exists():
            logger.info(f"Done with {tbl_prefix}. Continuing")
            continue

        out_fp.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting processing of {tbl_prefix}...")
        st = datetime.now()
        fn = functions[tbl_prefix]
        use_batched_loading = data_loader.should_batch(tbl_prefix, in_fp)
        if use_batched_loading:
            # Batched loading since Polars has trouble with ±2B rows in lazy mode, even with streaming.
            # This is a common issue for e.g., measurement and observation tables in large datasets.

            estimated_rows = data_loader.estimate_rows(in_fp)
            logger.info(
                f"Using batched loading for {tbl_prefix} (estimated rows={estimated_rows})"
            )

            temp_out_dir = out_fp.parent / f".{tbl_prefix}_parts"
            if temp_out_dir.exists():
                shutil.rmtree(temp_out_dir, ignore_errors=True)
            temp_out_dir.mkdir(parents=True, exist_ok=True)

            written_parts: list[Path] = []
            batch_iter = data_loader.iter_table_batches(tbl_prefix, in_fp)
            estimated_batches = data_loader.estimate_batches(in_fp)

            for batch_idx, df in enumerate(
                tqdm(
                    batch_iter,
                    desc=f"{tbl_prefix} batches",
                    unit="batch",
                    mininterval=5.0,
                    leave=False,
                    total=estimated_batches,
                ),
                start=1,
            ):
                processed_df = fn(df, concept_df, patient_df)
                processed_df = maybe_join_visit_occurrence_care_site(
                    tbl_prefix, processed_df
                )

                processed_df = processed_df.with_columns(table_name=pl.lit(tbl_prefix))
                part_fp = temp_out_dir / f"part_{batch_idx:05d}.parquet"
                processed_df.sink_parquet(part_fp, row_group_size=128_000)
                if part_fp.exists() and part_fp.stat().st_size > 0:
                    written_parts.append(part_fp)
                elif part_fp.exists():
                    part_fp.unlink()

            if not written_parts:
                logger.warning(
                    f"Skipping {tbl_prefix} as all processed batches were empty after preprocessing."
                )
                shutil.rmtree(temp_out_dir, ignore_errors=True)
                continue

            if len(written_parts) == 1:
                written_parts[0].replace(out_fp)
                shutil.rmtree(temp_out_dir, ignore_errors=True)
                logger.info(
                    f"Processed and wrote to {str(out_fp.resolve())} in {datetime.now() - st}"
                )
            else:
                temp_out_dir.replace(out_fp)
                logger.info(
                    f"Processed and wrote {len(written_parts)} parts to {str(out_fp.resolve())} in {datetime.now() - st}"
                )
        else:
            # Singular execution for smaller tables that Polars can handle
            df = data_loader.load_table(in_fp)
            if df is None:
                logger.warning(
                    f"Skipping {tbl_prefix} because no readable files were found."
                )
                continue

            processed_df = fn(df, concept_df, patient_df)
            if processed_df.limit(1).collect().is_empty():
                logger.warning(
                    f"Skipping {tbl_prefix} as it is empty after preprocessing (potentially due to filtering subjects)."
                )
                continue
            processed_df = maybe_join_visit_occurrence_care_site(
                tbl_prefix, processed_df
            )

            processed_df = processed_df.with_columns(table_name=pl.lit(tbl_prefix))
            if processed_df.limit(1).collect().is_empty():
                logger.warning(
                    f"Skipping {tbl_prefix} as it is empty after preprocessing (potentially due to filtering subjects)."
                )
                continue

            logger.info(
                f"{tbl_prefix}: rows before final sink={processed_df.select(pl.len()).collect().item(0, 0)}"
            )
            processed_df.sink_parquet(out_fp, row_group_size=128_000)

            logger.info(
                f"Processed and wrote to {str(out_fp.resolve())} in {datetime.now() - st}"
            )

    logger.info(
        f"Done! All dataframes processed and written to {str(MEDS_input_dir.resolve())}"
    )
    done_fp.write_text(f"completed_at={datetime.now().isoformat()}\n", encoding="utf-8")

    return
