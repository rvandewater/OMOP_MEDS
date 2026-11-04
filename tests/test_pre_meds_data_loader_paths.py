"""Loading paths of `pre_meds_data_loader` not exercised by the demo/e2e runs: parquet inputs, column case,
columns outside the OMOP schema, batching estimates and modes, and unreadable shards."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from omop_schema.utils import get_schema_loader

from OMOP_MEDS.pre_meds_data_loader import ShardedTableDataLoader, load_raw_file

OMOP_53 = get_schema_loader(5.3)
CARE_SITE_COLUMNS = [
    "care_site_id",
    "care_site_name",
    "place_of_service_concept_id",
    "location_id",
    "care_site_source_value",
    "place_of_service_source_value",
]


class _SchemaLoaderStub:
    def __init__(self, table_schemas: dict[str, pa.Schema]):
        self._table_schemas = table_schemas

    def get_pyarrow_schema(self, table_name: str) -> pa.Schema:
        return self._table_schemas[table_name]


MEASUREMENT_STUB = _SchemaLoaderStub(
    {
        "measurement": pa.schema(
            [pa.field("person_id", pa.int64()), pa.field("value", pa.int64())]
        )
    }
)


def _write_shards(table_dir: Path, row_counts: list[int]) -> list[Path]:
    table_dir.mkdir(parents=True, exist_ok=True)
    paths, offset = [], 0
    for idx, n_rows in enumerate(row_counts):
        fp = table_dir / f"part_{idx:04d}.parquet"
        pl.DataFrame(
            {"person_id": list(range(offset, offset + n_rows)), "value": [idx] * n_rows}
        ).write_parquet(fp)
        paths.append(fp)
        offset += n_rows
    return paths


def _write_garbage_parquet(fp: Path) -> Path:
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_bytes(b"this is not a parquet file")
    return fp


def _loader(**kwargs) -> ShardedTableDataLoader:
    return ShardedTableDataLoader(schema_loader=MEASUREMENT_STUB, **kwargs)


# --- load_raw_file: parquet inputs ------------------------------------------------------------------------


def test_single_parquet_file_is_cast_to_the_omop_schema_and_lowercased(tmp_path):
    fp = tmp_path / "care_site.parquet"
    pl.DataFrame(
        {
            "care_site_name": ["A", "B"],
            "care_site_id": [
                "1",
                "2",
            ],  # string ids in the source: cast to the OMOP Int64
            "location_id": [10, None],
            "EXTRA_Col": [1.5, 2.5],  # extra columns are kept, with lower-cased names
        }
    ).write_parquet(fp)

    df = load_raw_file(fp, OMOP_53).collect()

    assert df.schema == pl.Schema(
        {
            "care_site_name": pl.String,
            "care_site_id": pl.Int64,
            "location_id": pl.Int64,
            "extra_col": pl.Float64,
        }
    )
    assert df.rows() == [("A", 1, 10, 1.5), ("B", 2, None, 2.5)]


def test_parquet_directory_aligns_every_shard_and_keeps_columns_outside_the_schema(
    tmp_path,
):
    table_dir = tmp_path / "care_site"
    (table_dir / "nested").mkdir(parents=True)
    pl.DataFrame({"care_site_id": [1], "care_site_name": ["A"]}).write_parquet(
        table_dir / "part_0.parquet"
    )
    # A differently shaped shard further down the tree: other column order, Int32 id, string location_id,
    # and a column outside the OMOP schema.
    pl.DataFrame(
        {
            "care_site_name": ["B"],
            "care_site_id": pl.Series([2], dtype=pl.Int32),
            "location_id": ["7"],
            "not_in_omop": [1],
        }
    ).write_parquet(table_dir / "nested" / "part_1.parquet")

    df = load_raw_file(table_dir, OMOP_53).collect().sort("care_site_id")

    assert set(df.columns) == {*CARE_SITE_COLUMNS, "not_in_omop"}
    assert df["not_in_omop"].to_list() == [None, 1]
    assert df.schema["care_site_id"] == pl.Int64
    assert df.schema["location_id"] == pl.Int64
    assert df.select("care_site_id", "care_site_name", "location_id").rows() == [
        (1, "A", None),
        (2, "B", 7),
    ]
    assert df["place_of_service_concept_id"].null_count() == 2


def test_parquet_directory_spanning_several_shard_batches_keeps_every_row(tmp_path):
    # load_raw_file concatenates shards in batches of 64; 70 shards need two batches.
    table_dir = tmp_path / "observation_period"
    table_dir.mkdir()
    for i in range(70):
        pl.DataFrame(
            {"observation_period_id": [i], "person_id": [i % 5]}
        ).write_parquet(table_dir / f"part_{i:03d}.parquet")

    df = load_raw_file(table_dir, OMOP_53).collect()

    assert df.height == 70
    assert sorted(df["observation_period_id"]) == list(range(70))
    assert df.schema["observation_period_start_date"] == pl.Date


def _empty_dir(root: Path) -> Path:
    (root / "person").mkdir()
    return root / "person"


def _dir_without_data_files(root: Path) -> Path:
    table_dir = _empty_dir(root)
    (table_dir / "README.txt").write_text("not data")
    return table_dir


def _unsupported_suffix(root: Path) -> Path:
    fp = root / "person.json"
    fp.write_text("{}")
    return fp


@pytest.mark.parametrize(
    "make_input", [_empty_dir, _dir_without_data_files, _unsupported_suffix]
)
def test_inputs_without_readable_data_files_load_as_none(tmp_path, make_input):
    assert load_raw_file(make_input(tmp_path), OMOP_53) is None


# --- ShardedTableDataLoader: batching decisions ------------------------------------------------------------


def test_non_positive_row_threshold_batches_every_selected_table(tmp_path):
    table_dir = tmp_path / "measurement"
    _write_shards(table_dir, [1])

    assert _loader(
        chunked_tables=["measurement"], batching_row_threshold=0
    ).should_batch("measurement", table_dir)
    assert not _loader(chunked_tables=[], batching_row_threshold=0).should_batch(
        "measurement", table_dir
    )


def test_unreadable_shard_disables_row_estimate_and_batching(tmp_path):
    table_dir = tmp_path / "measurement"
    _write_shards(table_dir, [3])
    _write_garbage_parquet(table_dir / "part_9999.parquet")
    loader = _loader(chunked_tables=["measurement"], batching_row_threshold=0)

    assert loader.estimate_rows(table_dir) is None
    assert not loader.should_batch("measurement", table_dir)


def test_single_parquet_file_is_its_own_shard(tmp_path):
    (fp,) = _write_shards(tmp_path / "measurement", [4])
    loader = _loader(chunked_tables=["measurement"], batching_row_threshold=0)

    assert loader.estimate_rows(fp) == 4
    assert loader.estimate_batches(fp) == 1
    batches = list(loader.iter_table_batches("measurement", fp))
    assert [b.collect().height for b in batches] == [4]


def test_csv_input_has_no_row_estimate_so_is_never_batched(tmp_path):
    fp = tmp_path / "measurement.csv"
    fp.write_text("person_id,value\n1,10\n")
    loader = _loader(chunked_tables=["measurement"], batching_row_threshold=0)

    assert loader.estimate_rows(fp) is None
    assert not loader.should_batch("measurement", fp)


def test_non_batched_table_yields_exactly_one_frame(tmp_path):
    table_dir = tmp_path / "measurement"
    _write_shards(table_dir, [2, 3])

    (frame,) = list(
        _loader(chunked_tables=[]).iter_table_batches("measurement", table_dir)
    )

    assert frame.collect().sort("person_id")["person_id"].to_list() == [0, 1, 2, 3, 4]


def test_non_batched_table_without_readable_files_yields_nothing(tmp_path):
    empty_dir = tmp_path / "measurement"
    empty_dir.mkdir()

    assert list(_loader().iter_table_batches("measurement", empty_dir)) == []


class _AlwaysBatchLoader(ShardedTableDataLoader):
    def should_batch(self, table_name: str, fp: Path) -> bool:
        return True


def test_batched_table_without_parquet_shards_falls_back_to_one_frame(tmp_path):
    # A table selected for batching whose input is CSV has no parquet shards to batch by.
    fp = tmp_path / "measurement.csv"
    fp.write_text("person_id,value\n1,10\n2,20\n")
    loader = _AlwaysBatchLoader(schema_loader=MEASUREMENT_STUB)

    (frame,) = list(loader.iter_table_batches("measurement", fp))

    assert frame.collect().rows() == [(1, 10), (2, 20)]


def test_batched_table_whose_input_cannot_be_read_yields_nothing(tmp_path):
    loader = _AlwaysBatchLoader(schema_loader=MEASUREMENT_STUB)

    assert (
        list(loader.iter_table_batches("measurement", tmp_path / "measurement.txt"))
        == []
    )


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"batch_mode": "auto"}, "per_shard"),
        ({"batch_mode": "auto", "batch_size_shards": 3}, "by_shards"),
        (
            {"batch_mode": "auto", "batch_size_shards": 3, "batch_input_rows": 5},
            "by_rows",
        ),
        ({"batch_mode": "by_shards", "batch_input_rows": 5}, "by_shards"),
        ({"batch_mode": "something_else"}, "something_else"),
    ],
)
def test_effective_batch_mode(kwargs, expected):
    assert _loader(**kwargs)._effective_batch_mode() == expected


# --- ShardedTableDataLoader: batch estimates and boundaries -------------------------------------------------


def test_estimate_batches_without_parquet_files_is_none(tmp_path):
    assert _loader().estimate_batches(tmp_path / "missing") is None


@pytest.mark.parametrize(
    ("kwargs", "expected_batches"),
    [
        ({"batch_mode": "per_shard"}, [[0], [1], [2], [3], [4]]),
        ({"batch_mode": "by_shards", "batch_size_shards": 2}, [[0, 1], [2, 3], [4]]),
        # Shards hold 2, 3, 1, 4, 2 rows; a batch closes once it reaches 4 rows.
        ({"batch_mode": "by_rows", "batch_input_rows": 4}, [[0, 1], [2, 3], [4]]),
        # A non-positive row target degenerates to one shard per batch.
        ({"batch_mode": "by_rows", "batch_input_rows": 0}, [[0], [1], [2], [3], [4]]),
        # Unknown modes fall back to one shard per batch.
        ({"batch_mode": "something_else"}, [[0], [1], [2], [3], [4]]),
    ],
    ids=["per_shard", "by_shards", "by_rows", "by_rows_without_target", "unknown_mode"],
)
def test_batch_boundaries_and_estimates_agree(tmp_path, kwargs, expected_batches):
    table_dir = tmp_path / "measurement"
    shards = _write_shards(table_dir, [2, 3, 1, 4, 2])
    loader = _loader(chunked_tables=["measurement"], batching_row_threshold=0, **kwargs)

    batches = loader._build_batches(shards)

    assert [[shards.index(p) for p in b] for b in batches] == expected_batches
    assert loader.estimate_batches(table_dir) == len(expected_batches)
    frames = list(loader.iter_table_batches("measurement", table_dir))
    assert [f.collect()["value"].unique().sort().to_list() for f in frames] == [
        sorted(b) for b in expected_batches
    ]


def test_by_rows_counts_an_unreadable_shard_as_empty(tmp_path):
    table_dir = tmp_path / "measurement"
    good = _write_shards(table_dir, [2, 2])
    # Sorts first ("a_" < "part_"), so the batch it opens is filled by the readable shards.
    bad = _write_garbage_parquet(table_dir / "a_unreadable.parquet")
    loader = _loader(batch_mode="by_rows", batch_input_rows=3)

    shards = loader._list_parquet_files(table_dir)

    assert shards == [bad, *good]
    assert loader._build_batches(shards) == [[bad, *good]]
    assert loader.estimate_batches(table_dir) == 1


# --- column case and extension columns -------------------------------------------------------------------

NOTES = pl.DataFrame(
    {
        "note_id": [1, 2],
        "person_id": [1, 2],
        "note_date": ["2020-01-01", "2020-01-02"],
        "note_datetime": ["2020-01-01 08:00:00", "2020-01-02 08:00:00"],
        "note_text": ["a", "b"],
        "xtn_note_last_edit_datetime": ["2020-01-03 09:00:00", None],
    }
)


def test_uppercase_csv_headers_get_their_omop_types(tmp_path):
    # SQL Server exports write uppercase headers; their columns used to skip the schema and stay text.
    fp = tmp_path / "note.csv"
    NOTES.rename(str.upper).write_csv(fp)

    df = load_raw_file(fp, OMOP_53).collect()

    assert df.columns == NOTES.columns
    assert df.schema["note_id"] == pl.Int64
    assert df.schema["note_date"] == pl.Date
    assert df.schema["note_datetime"] == pl.Datetime("us")


@pytest.mark.parametrize("batched", [False, True])
def test_sharded_parquet_notes_keep_their_last_edit_extension_columns(
    tmp_path, batched
):
    # Dropping them silently gave notes their creation time, not their leakage-safe last-edit time.
    (tmp_path / "note").mkdir()
    for i in range(NOTES.height):
        NOTES[i : i + 1].write_parquet(tmp_path / "note" / f"part_{i}.parquet")
    loader = ShardedTableDataLoader(
        schema_loader=OMOP_53,
        chunked_tables=["note"],
        batching_row_threshold=0,
        batch_mode="per_shard",
    )

    frames = (
        list(loader.iter_table_batches("note", tmp_path / "note"))
        if batched
        else [load_raw_file(tmp_path / "note", OMOP_53)]
    )

    df = pl.concat([f.collect() for f in frames], how="diagonal").sort("note_id")
    assert df["xtn_note_last_edit_datetime"].to_list() == ["2020-01-03 09:00:00", None]
    assert len(frames) == (NOTES.height if batched else 1)
