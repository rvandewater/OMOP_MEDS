"""Performs pre-MEDS data wrangling for INSERT DATASET NAME HERE."""

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
        exit(0)
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

    chunked_tables = list(cfg.get("chunked_tables", ["measurement", "observation"]))
    data_loader = ShardedTableDataLoader(
        schema_loader=schema_loader,
        selector=selector,
        chunked_tables=chunked_tables,
        batching_row_threshold=int(cfg.get("batching_row_threshold", 1_000_000)),
        batch_mode=str(cfg.get("batch_mode", "auto")),
        batch_size_shards=int(cfg.get("batch_size_shards", 1)),
        batch_input_rows=int(cfg.get("batch_input_rows", 0)),
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

    for in_fp in all_fps:
        pfx = get_shard_prefix(OMOP_input_dir, in_fp)
        out_fp = MEDS_input_dir / f"{pfx}.parquet"

        if pfx in unused_tables:
            logger.warning(f"Skipping {pfx} as it is not supported in this pipeline.")
            continue
        elif pfx not in functions:
            if pfx in [
                "person",
                "death",
                "concept",
            ]:
                logger.info(
                    f"Skipping {pfx} as it has already been processed separately."
                )
            elif pfx in cfg.get("tables_to_ignore", []):
                logger.warning(
                    f"{pfx} will not be processed; this is seen as expected."
                )
            else:
                logger.warning(
                    f"No function needed for {pfx}. For {DATASET_NAME}, THIS IS COULD BE UNEXPECTED IF NOT ALREADY PROCESSED"
                )
            continue

        if out_fp.exists():
            logger.info(f"Done with {pfx}. Continuing")
            continue

        out_fp.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Processing {pfx}...")
        st = datetime.now()
        fn = functions[pfx]
        use_batched_loading = data_loader.should_batch(pfx, in_fp)
        if use_batched_loading:
            estimated_rows = data_loader.estimate_rows(in_fp)
            logger.info(
                f"Using batched loading for {pfx} (estimated rows={estimated_rows})"
            )

            temp_out_dir = out_fp.parent / f".{pfx}_parts"
            if temp_out_dir.exists():
                shutil.rmtree(temp_out_dir, ignore_errors=True)
            temp_out_dir.mkdir(parents=True, exist_ok=True)

            written_parts: list[Path] = []
            for batch_idx, df in enumerate(
                data_loader.iter_table_batches(pfx, in_fp), start=1
            ):
                processed_df = fn(df, concept_df, patient_df)
                if processed_df.limit(1).collect().is_empty():
                    continue

                if pfx == "visit_occurrence":
                    care_site_in_fp = get_table_path(OMOP_input_dir, "care_site")
                    if care_site_in_fp:
                        care_site_df = data_loader.load_table(care_site_in_fp)
                        if care_site_df is not None:
                            care_site_df = care_site_df.select(
                                ["care_site_id", "care_site_name"]
                            )
                            processed_df = processed_df.join(
                                care_site_df, on="care_site_id", how="left"
                            )

                processed_df = processed_df.with_columns(table_name=pl.lit(pfx))
                part_fp = temp_out_dir / f"part_{batch_idx:05d}.parquet"
                processed_df.sink_parquet(part_fp, row_group_size=128_000)
                if part_fp.exists() and part_fp.stat().st_size > 0:
                    written_parts.append(part_fp)
                elif part_fp.exists():
                    part_fp.unlink()

            if not written_parts:
                logger.warning(
                    f"Skipping {pfx} as all processed batches were empty after preprocessing."
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
            continue

        df = data_loader.load_table(in_fp)
        if df is None:
            logger.warning(f"Skipping {pfx} because no readable files were found.")
            continue

        processed_df = fn(df, concept_df, patient_df)
        if processed_df.limit(1).collect().is_empty():
            logger.warning(
                f"Skipping {pfx} as it is empty after preprocessing (potentially due to filtering subjects)."
            )
            continue
        if pfx == "visit_occurrence":
            care_site_in_fp = get_table_path(OMOP_input_dir, "care_site")
            if not care_site_in_fp:
                logger.warning(
                    "No care_site table found in the input directory. Skipping join with care_site."
                )
            else:
                logger.warning(f"Processed columns: {processed_df.collect_schema()}")
                care_site_df = data_loader.load_table(care_site_in_fp)
                if care_site_df is not None:
                    care_site_df = care_site_df.select(
                        ["care_site_id", "care_site_name"]
                    )
                    processed_df = processed_df.join(
                        care_site_df, on="care_site_id", how="left"
                    )

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
        # logger.info(
        #     f"processed_df schema: {processed_df.collect_schema()}"
        # )
        processed_df = processed_df.with_columns(table_name=pl.lit(pfx))
        if processed_df.limit(1).collect().is_empty():
            logger.warning(
                f"Skipping {pfx} as it is empty after preprocessing (potentially due to filtering subjects)."
            )
            continue

        # write_lazyframe(processed_df, out_fp)
        # if pfx == "visit_occurrence":
        #     # If this is the visit_occurrence table, we want to write it in a way that preserves the partitioning by visit_id for downstream processing efficiency
        #
        #     for shard, shard_df in processed_df.partition_by("shard_key", as_dict=True).items():
        #         shard_out_fp = out_fp.parent / f"{out_fp.stem}_{shard}.parquet"
        #         shard_df.sink_parquet(shard_out_fp)
        #         logger.info(f"Wrote shard {shard} to {shard_out_fp}")
        # else:

        logger.info(
            f"{pfx}: rows before final sink={processed_df.select(pl.len()).collect().item(0, 0)}"
        )
        processed_df.sink_parquet(out_fp, row_group_size=128_000)

        # processed_df.sink_parquet(out_fp)
        # if pfx == "measurement":
        #     shard_col = "person_id"
        #     n_buckets = 256
        #     processed_df = processed_df.with_columns(
        #         (pl.col(shard_col).hash() % n_buckets).alias("_bucket")
        #     )
        #     for b in range(n_buckets):
        #         out_b = out_fp.parent / f"{out_fp.stem}_part_{b:03d}.parquet"
        #         (
        #             processed_df
        #             .filter(pl.col("_bucket") == b)
        #             .drop("_bucket")
        #             .sink_parquet(out_b)
        #         )
        # else:
        #     processed_df.sink_parquet(out_fp)
        logger.info(
            f"Processed and wrote to {str(out_fp.resolve())} in {datetime.now() - st}"
        )

    logger.info(
        f"Done! All dataframes processed and written to {str(MEDS_input_dir.resolve())}"
    )
    done_fp.write_text(f"completed_at={datetime.now().isoformat()}\n", encoding="utf-8")

    return


def sink_partitioned_by_hash_bucket(
    lf: pl.LazyFrame,
    out_dir: Path,
    bucket_col: str,
    n_buckets: int = 256,
    row_group_size: int = 128_000,
) -> list[Path]:
    """
    Write a large LazyFrame in bounded chunks by hashing `bucket_col`.
    This avoids executing one huge sink plan that can OOM/segfault.

    Returns the list of created parquet files.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    # Build once; each bucket is a filtered sub-plan.
    bucketed = lf.with_columns(
        (pl.col(bucket_col).hash(seed=0) % pl.lit(n_buckets)).alias("_bucket")
    )

    for b in tqdm(range(n_buckets), desc="writing buckets", unit="bucket"):
        out_fp = out_dir / f"part_{b:04d}.parquet"
        (
            bucketed.filter(pl.col("_bucket") == pl.lit(b))
            .drop("_bucket")
            .sink_parquet(out_fp, row_group_size=row_group_size)
        )
        # Keep only non-empty outputs
        try:
            if out_fp.exists() and out_fp.stat().st_size > 0:
                written.append(out_fp)
            elif out_fp.exists():
                out_fp.unlink()
        except FileNotFoundError:
            pass

    return written
