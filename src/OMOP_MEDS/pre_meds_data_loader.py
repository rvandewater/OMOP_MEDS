import math
from pathlib import Path
from typing import Iterator
import polars as pl
from omop_schema.scan import scan_table
from omop_schema.schema.base import OMOPSchemaBase
from polars import selectors as cs
from polars._typing import SelectorType
from pyarrow import parquet as pq


def _is_parquet_shard_directory(fp: Path) -> bool:
    """Whether `fp` is a directory read from its Parquet shards (a directory with CSV shards is read as CSV)."""
    if not fp.is_dir():
        return False
    names = [p.name.lower() for p in fp.glob("**/*") if p.is_file()]
    return not any(n.endswith((".csv", ".csv.gz")) for n in names) and any(
        n.endswith(".parquet") for n in names
    )


def load_raw_file(
    fp: Path, schema_loader: OMOPSchemaBase, selector: SelectorType = cs.all()
) -> pl.LazyFrame | None:
    """Lazily read the OMOP table at `fp` in its OMOP schema, or return None if it holds no data files.

    Because OMOP tables can be quite large for datasets comprising millions
    of subjects, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. `fp` is a `.csv`,
    `.csv.gz` or `.parquet` file, or such a folder, named after its OMOP table;
    `omop_schema.scan.scan_table` reads it.

    Examples:
        >>> from pathlib import Path
        >>> import polars as pl
        >>> from omop_schema.utils import get_schema_loader
        >>> schema_loader = get_schema_loader(5.3)
        >>> fp = Path("tests/demo_resources/observation.csv")
        >>> df = load_raw_file(fp, schema_loader)
    """
    # Parquet shards of large exports drift in type from shard to shard, so they are cast permissively, and
    # given every schema column, as before; CSV values and single files must parse. Columns outside the schema,
    # such as the note last-edit times that keep notes from leaking, are kept either way.
    shard_directory = _is_parquet_shard_directory(fp)
    return scan_table(
        fp,
        schema_loader,
        selector=selector,
        strict=not shard_directory,
        add_missing_columns=shard_directory,
    )


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
        # Every schema column in every batch, so the parts written from them share one layout.
        return scan_table(
            parquet_paths,
            self.schema_loader,
            table_name=table_name,
            selector=self.selector,
            strict=False,
            add_missing_columns=True,
        )
