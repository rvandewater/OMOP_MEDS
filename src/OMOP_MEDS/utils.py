from pathlib import Path

import polars as pl

from OMOP_MEDS.__main__ import logger, _ensure_parent_dir


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
