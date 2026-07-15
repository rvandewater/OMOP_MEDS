"""Utilities for extracting MEDS-extract row metadata sidecars."""

from pathlib import Path

import polars as pl

MEDS_EXTRACT_LINK_ID = "meds_extract_link_id"
ROW_IDX = "meds_extract_row_idx"

DEFAULT_METADATA_COLUMNS: tuple[str, ...] = (
    MEDS_EXTRACT_LINK_ID,
    ROW_IDX,
    "source_block",
    "code_components",
)

DEFAULT_DTYPES: dict[str, pl.DataType] = {
    MEDS_EXTRACT_LINK_ID: pl.Int64,
    ROW_IDX: pl.UInt32,
    "source_block": pl.Utf8,
}


def _link_id_expr(rel_fp: Path) -> pl.Expr:
    return (
        pl.struct(pl.lit(str(rel_fp)).alias("shard"), pl.col(ROW_IDX))
        .hash(seed=0)
        .mod(2**63 - 1)
        .cast(pl.Int64)
        .alias(MEDS_EXTRACT_LINK_ID)
    )


def build_meds_extract_linked_data(
    meds_cohort_dir: str | Path,
    *,
    output_dir: str | Path | None = None,
    columns: tuple[str, ...] = DEFAULT_METADATA_COLUMNS,
    overwrite: bool = False,
    update_data: bool = True,
) -> list[Path]:
    """Move per-row MEDS-extract metadata into sidecars.

    Output mirrors ``MEDS_cohort/data`` under
    ``MEDS_cohort/metadata/meds_extract_data``. Each output row corresponds to
    the row at the same index in the matching data shard. By default, the main
    data shards are rewritten with ``meds_extract_link_id`` and without
    ``source_block``/``code_components``.

    Args:
        meds_cohort_dir: Path to the MEDS cohort directory.
        output_dir: Optional path to write the linked data sidecars. If not provided, defaults to ``MEDS_cohort/metadata/meds_extract_data``.
        columns: Tuple of column names to include in the output sidecars. Defaults to ``("meds_extract_link_id", "meds_extract_row_idx", "source_block", "code_components")``.
        overwrite: If True, existing linked data sidecars will be overwritten. Defaults to False.
        update_data: If True, the main data shards will be rewritten with ``meds_extract_link_id`` and without ``source_block``/``code_components``. Defaults to True.
    """
    meds_cohort_dir = Path(meds_cohort_dir)
    data_dir = meds_cohort_dir / "data"
    output_dir = (
        Path(output_dir)
        if output_dir
        else meds_cohort_dir / "metadata" / "meds_extract_data"
    )

    if not data_dir.exists():
        raise FileNotFoundError(f"MEDS data directory not found: {data_dir}")
    if output_dir.exists() and any(output_dir.rglob("*.parquet")) and not overwrite:
        raise FileExistsError(
            f"Linked data already exists at {output_dir}; pass overwrite=True to replace it"
        )
    if overwrite and output_dir.exists():
        for fp in output_dir.rglob("*.parquet"):
            fp.unlink()

    data_files = sorted(data_dir.rglob("*.parquet"))
    written: list[Path] = []
    for in_fp in data_files:
        rel_fp = in_fp.relative_to(data_dir)
        out_fp = output_dir / rel_fp
        out_fp.parent.mkdir(parents=True, exist_ok=True)

        lf = pl.scan_parquet(in_fp, row_index_name=ROW_IDX)
        schema = lf.collect_schema()
        id_expr = _link_id_expr(rel_fp)

        exprs = []
        for col in columns:
            dtype = DEFAULT_DTYPES.get(col)
            if col == MEDS_EXTRACT_LINK_ID:
                exprs.append(id_expr)
            elif col in schema:
                expr = pl.col(col)
                if dtype is not None:
                    expr = expr.cast(dtype, strict=False)
                exprs.append(expr.alias(col))
            else:
                exprs.append(
                    pl.lit(None, dtype=dtype).alias(col)
                    if dtype
                    else pl.lit(None).alias(col)
                )

        lf.select(exprs).sink_parquet(out_fp)
        written.append(out_fp)

        if update_data:
            keep_cols = [
                col
                for col in schema.names()
                if col
                not in {
                    "source_block",
                    "code_components",
                    MEDS_EXTRACT_LINK_ID,
                    ROW_IDX,
                }
            ]
            tmp_fp = in_fp.with_suffix(f"{in_fp.suffix}.tmp")
            if tmp_fp.exists():
                tmp_fp.unlink()
            lf.select(*keep_cols, id_expr).sink_parquet(tmp_fp)
            tmp_fp.replace(in_fp)

    if not written:
        raise ValueError(f"No parquet shards found under {data_dir}")
    return written
