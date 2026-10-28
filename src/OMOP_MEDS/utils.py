import logging
from pathlib import Path
import shutil

import polars as pl

logger = logging.getLogger(__name__)


def finish_codes_metadata(MEDS_cohort_dir: Path, pre_MEDS_dir: Path):
    codes_source = pre_MEDS_dir / "codes.parquet"
    codes_dest = MEDS_cohort_dir / "metadata/codes.parquet"

    if codes_source.exists():
        MEDS_cohort_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Copying codes.parquet from {codes_source} to {codes_dest}")
        # Read metadata and collected codes
        metadata = pl.read_parquet(codes_source)
        collected = (
            pl.scan_parquet(MEDS_cohort_dir / "data/**/*.parquet")
            .select(["code", "table_name"])
            .group_by(["code", "table_name"])
            .agg(pl.len().alias("occurrence_count"))
            .collect()
        )
        # Extract base code for alignment
        collected = collected.with_columns(
            pl.col("code").str.replace(r"(//start|//end)$", "").alias("base_code")
        )
        metadata = metadata.with_columns(pl.col("code").alias("base_code"))
        # Join to fill metadata for suffixed codes
        merged = collected.join(metadata, on="base_code", how="left").with_columns(
            pl.col("code")  # keep original code with suffix
        )
        # Save merged codes
        _ensure_parent_dir(codes_dest)
        merged.write_parquet(codes_dest)
    else:
        logger.warning(f"codes.parquet not found in {pre_MEDS_dir}")


def _ensure_parent_dir(path: Path) -> None:
    """Ensure parent directory exists for a file path."""
    path.parent.mkdir(parents=True, exist_ok=True)


def remove_intermediate_files(
    MEDS_cohort_dir: Path,
    pre_MEDS_dir: Path,
    remove_pre_MEDS: bool = False,
    remove_MEDS_cohort: bool = True,
):
    """Remove intermediate files from the MEDS cohort directory."""
    intermediate_folders = []
    if remove_pre_MEDS and pre_MEDS_dir.exists():
        logger.info(f"Removing entire pre-MEDS directory: {pre_MEDS_dir}")
        shutil.rmtree(pre_MEDS_dir)
    if remove_MEDS_cohort:
        intermediate_folders = [
            MEDS_cohort_dir / "convert_to_MEDS_events/",
            MEDS_cohort_dir / "convert_to_subject_sharded/",
            MEDS_cohort_dir / "split_and_shard_subjects/",
            MEDS_cohort_dir / "shard_events/",
            MEDS_cohort_dir / "merge_to_MEDS_cohort/",
            MEDS_cohort_dir / "finalize_MEDS_metadata/",
            MEDS_cohort_dir / "extract_code_metadata/",
        ]
    for file_path in intermediate_folders:
        if file_path.is_dir():
            logger.info(f"Removing intermediate directory: {file_path}")
            shutil.rmtree(file_path)
        else:
            logger.warning(f"Intermediate directory not found: {file_path}")
