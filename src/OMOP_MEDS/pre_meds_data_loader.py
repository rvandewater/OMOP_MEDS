import logging
import math
from pathlib import Path
from typing import Iterator, Optional
import polars as pl
from loguru import logger
from omop_schema.convert import convert_to_schema_polars
from omop_schema.schema.base import OMOPSchemaBase
from omop_schema.utils import pyarrow_to_polars_schema
from polars import selectors as cs
from polars._typing import SelectorType
from pyarrow import parquet as pq


def load_raw_file(
    fp: Path, schema_loader: OMOPSchemaBase, selector: SelectorType = cs.all()
) -> pl.LazyFrame | None:
    """Retrieve all .csv/.csv.gz/.parquet files for the OMOP table given by fp

    Because OMOP tables can be quite large for datasets comprising millions
    of subjects, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. This function
    takes a path corresponding to an OMOP table with its standard name (e.g.,
    `condition_occurrence`, `measurement`, `observation`) and returns two list
    of paths.

    The first list contains all the csv files. The second list contains all parquet files.

    Examples:
        >>> from pathlib import Path
        >>> import polars as pl
        >>> from omop_schema.utils import get_schema_loader
        >>> schema_loader = get_schema_loader(5.3)
        >>> fp = Path("tests/demo_resources/observation.csv")
        >>> df = load_raw_file(fp, schema_loader)
    """
    # TODO: Write tool/method that reads a specific omop table with a specific datatypes
    table_name = fp.stem.split(".")[0]  # Infer table name from file path
    schema = pyarrow_to_polars_schema(schema_loader.get_pyarrow_schema(table_name))

    # pa.schema([(col, dtype) for coll, dtype in schema.items()])
    # Convert dict to pa.Schema
    if fp.suffixes == [".csv", ".gz"]:
        file = pl.scan_csv(
            fp, compression="gzip", infer_schema=False, schema_overrides=schema
        ).select(selector)
        logging.info(f"Loaded gzipped CSV file from {fp}")
    elif fp.suffix == ".csv":
        # Using schema_overrides to set the schema as the ordering could be different
        # and there could be extra columns
        file = pl.scan_csv(
            fp, infer_schema=False, has_header=True, schema_overrides=schema
        ).select(selector)
        logging.info(f"Loaded CSV file from {fp}")
    elif fp.suffix == ".parquet":
        file = pl.scan_parquet(fp).select(selector)
        file = convert_to_schema_polars(file, schema, allow_extra_columns=True)
        logging.info(f"Loaded Parquet file from {fp}")
    elif fp.is_dir():
        files = list(fp.glob("**/*"))
        csv_files = [file for file in files if file.suffix in [".csv", ".gz"]]
        parquet_files = [file for file in files if file.suffix == ".parquet"]
        # mismatching_schema_check = True
        if csv_files:
            file = pl.scan_csv(
                fp, infer_schema=False, has_header=True, schema_overrides=schema
            ).select(selector)
            logging.info(f"Loaded CSV files as directory from {fp}")
        elif parquet_files:
            # Memory-safe schema harmonization across shards
            # file = scan_harmonized(fp, glob="**/*.parquet")
            # # Keep only requested columns and cast to OMOP schema permissively
            # file = file.select(selector)
            # file = convert_to_schema_polars(
            #     file, schema, allow_extra_columns=True, allow_missing_columns=True
            # )
            # Per-shard projection/cast to avoid building one huge harmonized concat plan.
            # shard_lfs: list[pl.LazyFrame] = []
            # for shard_fp in sorted(parquet_files):
            #     shard_lf = pl.scan_parquet(shard_fp).select(selector)
            #     shard_lf = project_to_target_schema(shard_lf, schema)
            #     shard_lfs.append(shard_lf)
            #
            # if not shard_lfs:
            #     return None
            # Safer for very large dirs: build aligned shards in bounded batches.
            # This avoids one giant concat graph that can OOM/panic native Polars.
            parquet_files = sorted(parquet_files)
            batch_size = 64  # tune 32-128 based on memory
            batch_lfs: list[pl.LazyFrame] = []

            # Restrict schema to selected columns to lower memory.
            # selector may be a callable selector; this path keeps full schema if unsure.
            target_schema = schema

            for i in range(0, len(parquet_files), batch_size):
                chunk = parquet_files[i : i + batch_size]
                shard_lfs: list[pl.LazyFrame] = []

                for shard_fp in chunk:
                    shard = pl.scan_parquet(shard_fp, rechunk=False).select(selector)
                    shard = _align_shard_to_schema(shard, target_schema)
                    shard_lfs.append(shard)

                if shard_lfs:
                    batch_lfs.append(pl.concat(shard_lfs, how="vertical_relaxed"))

            if not batch_lfs:
                return None
            # vertical_relaxed allows minor dtype reconciliation without exploding memory.
            file = pl.concat(batch_lfs, how="vertical_relaxed")
            logging.info(
                f"Loaded Parquet files as directory from {fp} of {len(parquet_files)} shards"
            )

            # # pl.scan_parquet(fp)
            # file = pl.scan_parquet(fp).select(
            #     selector
            # )  # , schema=schema, allow_missing_columns=True)
            # # if mismatching_schema_check:
            # #     cast_files_to_schema(str(fp), schema, str(fp))
            # file = convert_to_schema_polars(file, schema, allow_extra_columns=False)
            # logging.info(f"Loaded Parquet files as directory from {fp}")
        else:
            return None
    else:
        return None
    file = file.select(pl.all().name.to_lowercase())
    # logging.info(f"Loaded {file} with schema: {file.collect_columns}")
    return file


class ShardedTableDataLoader:
    """Load OMOP tables with optional shard-wise batching for selected tables.

    For non-selected tables this class defers to ``load_raw_file`` to preserve
    existing behavior. For selected parquet directory tables, it can yield data
    in bounded batches to lower peak memory.
    """

    def __init__(
        self,
        schema_loader: OMOPSchemaBase,
        selector: SelectorType = cs.all(),
        chunked_tables: list[str] | None = None,
        batching_row_threshold: int = 1_000_000,
        batch_mode: str = "auto",
        batch_size_shards: int = 1,
        batch_input_rows: int = 0,
    ) -> None:
        """
        Initializes the ShardedTableDataLoader.

        Args:
            schema_loader (OMOPSchemaBase): The schema loader instance to retrieve table schemas.
            selector (SelectorType, optional): A column selector to filter columns during loading. Defaults to `cs.all()`.
            chunked_tables (list[str] | None, optional): A list of table names that are eligible to be processed in chunks. Defaults to None.
            batching_row_threshold (int, optional): The row count threshold for enabling batching. Defaults to 1,000,000.
            batch_mode (str, optional): The batching mode. Can be "auto", "per_shard", "by_shards", or "by_rows". Defaults to "auto".
            batch_size_shards (int, optional): The number of shards to include in each batch when using "by_shards" mode. Defaults to 1.
            batch_input_rows (int, optional): The maximum number of rows per batch when using "by_rows" mode. Defaults to 0 (disabled).

        Returns:
            None
        """
        self.schema_loader = schema_loader
        self.selector = selector
        self.chunked_tables = set(chunked_tables or [])
        self.batching_row_threshold = batching_row_threshold
        self.batch_mode = batch_mode
        self.batch_size_shards = max(1, int(batch_size_shards))
        self.batch_input_rows = max(0, int(batch_input_rows))

    def load_table(self, fp: Path) -> pl.LazyFrame | None:
        """Load a table with existing non-batched semantics."""
        return load_raw_file(fp, self.schema_loader, self.selector)

    def should_batch(self, table_name: str, fp: Path) -> bool:
        """Return whether batching should be used for this table/path."""
        if table_name not in self.chunked_tables:
            return False

        estimated_rows = self.estimate_rows(fp)
        if estimated_rows is None:
            return False

        if self.batching_row_threshold <= 0:
            return True
        return estimated_rows > self.batching_row_threshold

    def iter_table_batches(self, table_name: str, fp: Path) -> Iterator[pl.LazyFrame]:
        """Yield one or more LazyFrame batches for a table.

        If batching is not enabled (or not applicable), yields exactly one frame.
        """
        if not self.should_batch(table_name, fp):
            lf = self.load_table(fp)
            if lf is not None:
                yield lf
            return

        parquet_files = self._list_parquet_files(fp)
        if not parquet_files:
            lf = self.load_table(fp)
            if lf is not None:
                yield lf
            return

        for batch_files in self._build_batches(parquet_files):
            yield self._scan_parquet_batch(batch_files, table_name)

    def estimate_rows(self, fp: Path) -> int | None:
        """Estimate total row count using parquet metadata only."""
        parquet_files = self._list_parquet_files(fp)
        if not parquet_files:
            return None

        total = 0
        for path in parquet_files:
            try:
                total += pq.ParquetFile(path).metadata.num_rows
            except Exception:
                return None
        return total

    def estimate_batches(self, fp: Path) -> int | None:
        """Estimate total batch count using parquet metadata only."""
        parquet_files = self._list_parquet_files(fp)
        if not parquet_files:
            return None

        mode = self._effective_batch_mode()

        if mode == "per_shard":
            return len(parquet_files)

        if mode == "by_shards":
            step = max(1, self.batch_size_shards)
            return math.ceil(len(parquet_files) / step)

        if mode == "by_rows":
            if self.batch_input_rows <= 0:
                return len(parquet_files)

            batches = 0
            current_rows = 0
            for path in parquet_files:
                try:
                    shard_rows = pq.ParquetFile(path).metadata.num_rows
                except Exception:
                    shard_rows = 0

                if current_rows > 0 and current_rows >= self.batch_input_rows:
                    batches += 1
                    current_rows = 0

                current_rows += shard_rows

            if current_rows > 0:
                batches += 1
            return batches

        return len(parquet_files)

    def _list_parquet_files(self, fp: Path) -> list[Path]:
        if fp.is_file() and fp.suffix == ".parquet":
            return [fp]
        if fp.is_dir():
            return sorted(p for p in fp.glob("**/*.parquet") if p.is_file())
        return []

    def _effective_batch_mode(self) -> str:
        if self.batch_mode == "auto":
            if self.batch_input_rows > 0:
                return "by_rows"
            if self.batch_size_shards > 1:
                return "by_shards"
            return "per_shard"
        return self.batch_mode

    def _build_batches(self, parquet_files: list[Path]) -> list[list[Path]]:
        mode = self._effective_batch_mode()

        if mode == "per_shard":
            return [[p] for p in parquet_files]

        if mode == "by_shards":
            step = self.batch_size_shards
            return [
                parquet_files[i : i + step] for i in range(0, len(parquet_files), step)
            ]

        if mode == "by_rows":
            if self.batch_input_rows <= 0:
                return [[p] for p in parquet_files]

            batches: list[list[Path]] = []
            current: list[Path] = []
            current_rows = 0
            for path in parquet_files:
                try:
                    shard_rows = pq.ParquetFile(path).metadata.num_rows
                except Exception:
                    shard_rows = 0

                if current and current_rows >= self.batch_input_rows:
                    batches.append(current)
                    current = []
                    current_rows = 0

                current.append(path)
                current_rows += shard_rows

            if current:
                batches.append(current)
            return batches

        return [[p] for p in parquet_files]

    def _scan_parquet_batch(
        self, parquet_paths: list[Path], table_name: str
    ) -> pl.LazyFrame:
        schema = pyarrow_to_polars_schema(
            self.schema_loader.get_pyarrow_schema(table_name)
        )
        shard_lfs: list[pl.LazyFrame] = []
        for shard_fp in parquet_paths:
            shard = pl.scan_parquet(shard_fp, rechunk=False)
            shard = _align_shard_to_schema(shard, schema)
            shard = shard.select(self.selector)
            shard_lfs.append(shard)

        if len(shard_lfs) == 1:
            file = shard_lfs[0]
        else:
            file = pl.concat(shard_lfs, how="vertical_relaxed")
        return file.select(pl.all().name.to_lowercase())


def _resolve_conflict(dtypes: set[pl.DataType]) -> pl.DataType:
    """Given a set of conflicting types for the same column, pick a safe common type."""
    if len(dtypes) == 1:
        return next(iter(dtypes))

    # Decimal[p1,s] vs Decimal[p2,s] → Float64 (covers your exact error)
    if any(d.is_decimal() for d in dtypes):
        return pl.Float64

    # Mixed int widths → widest signed int, or Float64 if floats involved
    if all(d.is_numeric() for d in dtypes):
        if any(d.is_float() for d in dtypes):
            return pl.Float64
        # All integers: pick widest
        int_widths = {
            pl.Int8: 8,
            pl.Int16: 16,
            pl.Int32: 32,
            pl.Int64: 64,
            pl.UInt8: 8,
            pl.UInt16: 16,
            pl.UInt32: 32,
            pl.UInt64: 64,
        }
        return max((d for d in dtypes if d in int_widths), key=lambda d: int_widths[d])

    # Datetime with differing time_unit or time_zone → normalize to us / UTC
    if all(isinstance(d, pl.Datetime) for d in dtypes):
        return pl.Datetime("us", "UTC")

    # Fallback: String is always safe
    return pl.String


def collect_shard_schemas(
    paths: list[Path],
) -> dict[Path, dict[str, pl.DataType]]:
    """Read only parquet footer metadata — zero data loaded."""
    return {p: pl.read_parquet_schema(p) for p in paths}


def resolve_target_schema(
    shard_schemas: dict[Path, dict[str, pl.DataType]],
    keep_columns: Optional[list[str]] = None,
) -> dict[str, pl.DataType]:
    """
    Build a unified target schema from all shard schemas.
    - Union of all columns (shards missing a column will get a null literal)
    - Type conflicts resolved via _resolve_conflict()
    - If keep_columns is given, output is restricted to those columns
    """
    col_types: dict[str, set[pl.DataType]] = {}
    for schema in shard_schemas.values():
        for col, dtype in schema.items():
            col_types.setdefault(col, set()).add(dtype)

    target = {col: _resolve_conflict(types) for col, types in col_types.items()}

    if keep_columns:
        missing = set(keep_columns) - set(target)
        if missing:
            raise ValueError(f"Requested columns not found in any shard: {missing}")
        target = {c: target[c] for c in keep_columns}

    return target


def harmonize_shard(
    path: Path,
    target_schema: dict[str, pl.DataType],
    shard_schema: dict[str, pl.DataType],
) -> pl.LazyFrame:
    """
    Return a LazyFrame for one shard that:
      - adds missing columns as typed nulls
      - drops extra columns not in target_schema
      - casts columns whose type differs from target
    All operations are lazy — nothing is collected.
    """
    lf = pl.scan_parquet(path)
    exprs: list[pl.Expr] = []

    for col, target_dtype in target_schema.items():
        if col not in shard_schema:
            # Column absent in this shard → null literal
            exprs.append(pl.lit(None).cast(target_dtype).alias(col))
        elif shard_schema[col] != target_dtype:
            # Type mismatch → cast (use try_cast to avoid hard failures on bad values)
            exprs.append(pl.col(col).cast(target_dtype, strict=False).alias(col))
        else:
            exprs.append(pl.col(col))

    return lf.select(exprs)


def scan_harmonized(
    directory: str | Path,
    glob: str = "**/*.parquet",
    keep_columns: Optional[list[str]] = None,
    schema_sample: Optional[int] = None,
) -> pl.LazyFrame:
    """
    Lazily scan all parquet shards in a directory, harmonizing schemas.

    Args:
        directory:     Root folder containing sharded parquet files.
        glob:          Glob pattern to find shards (default: all parquet recursively).
        keep_columns:  If set, only these columns are retained in the output.
        schema_sample: If set, only inspect the first N shards for schema inference
                       (useful when there are thousands of shards with stable schemas).

    Returns:
        A single lazy LazyFrame with a unified, harmonized schema.
    """
    paths = sorted(Path(directory).glob(glob))
    if not paths:
        raise FileNotFoundError(
            f"No parquet files found in {directory!r} with {glob!r}"
        )

    sample = paths[:schema_sample] if schema_sample else paths
    shard_schemas = collect_shard_schemas(sample)
    target_schema = resolve_target_schema(shard_schemas, keep_columns=keep_columns)

    # Extend target to all paths (not just the sample) using cached schemas
    all_schemas = shard_schemas if not schema_sample else collect_shard_schemas(paths)

    # Build schemas for all paths, using target_schema as fallback for unsampled paths
    all_schemas = {}
    for path in paths:
        if path in shard_schemas:
            all_schemas[path] = shard_schemas[path]
        else:
            all_schemas[path] = target_schema

    frames = [harmonize_shard(path, target_schema, all_schemas[path]) for path in paths]
    logger.info(
        f"Harmonized {len(frames)} shards in {directory!r} with target schema: {target_schema}"
    )
    return pl.concat(frames, how="diagonal_relaxed")


def project_to_target_schema(
    lf: pl.LazyFrame, target_schema: dict[str, pl.DataType]
) -> pl.LazyFrame:
    existing = set(lf.collect_schema().names())
    exprs: list[pl.Expr] = []
    for col, dtype in target_schema.items():
        if col in existing:
            exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
        else:
            exprs.append(pl.lit(None, dtype=dtype).alias(col))
    return lf.select(exprs)


def _align_shard_to_schema(
    lf: pl.LazyFrame, target_schema: dict[str, pl.DataType]
) -> pl.LazyFrame:
    """Project a shard to exactly target schema (add missing as null, cast permissively)."""
    existing = set(lf.collect_schema().names())
    exprs: list[pl.Expr] = []
    for col, dtype in target_schema.items():
        if col in existing:
            exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
        else:
            exprs.append(pl.lit(None).cast(dtype).alias(col))
    return lf.select(exprs)
