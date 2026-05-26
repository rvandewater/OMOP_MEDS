from pathlib import Path

import polars as pl
import pyarrow as pa

from OMOP_MEDS.pre_meds_utils import ShardedTableDataLoader


class _SchemaLoaderStub:
    def __init__(self, table_schemas: dict[str, pa.Schema]):
        self._table_schemas = table_schemas

    def get_pyarrow_schema(self, table_name: str) -> pa.Schema:
        return self._table_schemas[table_name]


def _write_parquet_shards(
    base_dir: Path, table_name: str, row_counts: list[int]
) -> Path:
    table_dir = base_dir / table_name
    table_dir.mkdir(parents=True, exist_ok=True)

    offset = 0
    for idx, n_rows in enumerate(row_counts):
        df = pl.DataFrame(
            {
                "person_id": list(range(offset, offset + n_rows)),
                "value": list(range(n_rows)),
            }
        )
        df.write_parquet(table_dir / f"part_{idx:04d}.parquet")
        offset += n_rows

    return table_dir


def _build_loader(
    chunked_tables: list[str],
    batching_row_threshold: int,
    batch_mode: str = "auto",
    batch_size_shards: int = 1,
    batch_input_rows: int = 0,
) -> ShardedTableDataLoader:
    schema_loader = _SchemaLoaderStub(
        {
            "measurement": pa.schema(
                [
                    pa.field("person_id", pa.int64()),
                    pa.field("value", pa.int64()),
                ]
            )
        }
    )
    return ShardedTableDataLoader(
        schema_loader=schema_loader,
        selector=pl.selectors.all(),
        chunked_tables=chunked_tables,
        batching_row_threshold=batching_row_threshold,
        batch_mode=batch_mode,
        batch_size_shards=batch_size_shards,
        batch_input_rows=batch_input_rows,
    )


def test_should_batch_uses_metadata_row_estimate(tmp_path: Path):
    table_dir = _write_parquet_shards(
        tmp_path, "measurement", [300_000, 400_000, 500_000]
    )
    loader = _build_loader(
        chunked_tables=["measurement"],
        batching_row_threshold=1_000_000,
        batch_mode="auto",
    )

    assert loader.estimate_rows(table_dir) == 1_200_000
    assert loader.should_batch("measurement", table_dir)


def test_should_batch_false_for_non_chunked_table(tmp_path: Path):
    table_dir = _write_parquet_shards(tmp_path, "measurement", [10, 20])
    loader = _build_loader(
        chunked_tables=["observation"],
        batching_row_threshold=1,
        batch_mode="auto",
    )

    assert not loader.should_batch("measurement", table_dir)


def test_iter_table_batches_per_shard_mode(tmp_path: Path):
    table_dir = _write_parquet_shards(tmp_path, "measurement", [3, 4, 5])
    loader = _build_loader(
        chunked_tables=["measurement"],
        batching_row_threshold=1,
        batch_mode="per_shard",
    )

    batches = list(loader.iter_table_batches("measurement", table_dir))
    assert len(batches) == 3
    assert [b.select(pl.len()).collect().item(0, 0) for b in batches] == [3, 4, 5]


def test_iter_table_batches_by_shards_mode(tmp_path: Path):
    table_dir = _write_parquet_shards(tmp_path, "measurement", [2, 2, 2, 2, 2])
    loader = _build_loader(
        chunked_tables=["measurement"],
        batching_row_threshold=1,
        batch_mode="by_shards",
        batch_size_shards=2,
    )

    batches = list(loader.iter_table_batches("measurement", table_dir))
    assert len(batches) == 3
    assert [b.select(pl.len()).collect().item(0, 0) for b in batches] == [4, 4, 2]


def test_iter_table_batches_auto_prefers_row_target(tmp_path: Path):
    table_dir = _write_parquet_shards(tmp_path, "measurement", [3, 4, 2])
    loader = _build_loader(
        chunked_tables=["measurement"],
        batching_row_threshold=1,
        batch_mode="auto",
        batch_size_shards=3,
        batch_input_rows=5,
    )

    batches = list(loader.iter_table_batches("measurement", table_dir))
    assert len(batches) == 2
    assert [b.select(pl.len()).collect().item(0, 0) for b in batches] == [7, 2]
