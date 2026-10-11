from pathlib import Path

import polars as pl
import pytest

from OMOP_MEDS.linked_data import build_meds_extract_linked_data


def test_build_meds_extract_linked_data_writes_metadata_sidecars(tmp_path: Path):
    meds_dir = tmp_path / "MEDS_cohort"
    data_dir = meds_dir / "data"
    (data_dir / "train").mkdir(parents=True)
    (data_dir / "held_out").mkdir(parents=True)

    code_components = pl.Series(
        "code_components",
        [{"preferred_concept_name": "44814638"}, {"preferred_concept_name": "123"}],
        dtype=pl.Struct({"preferred_concept_name": pl.Utf8}),
    )
    pl.DataFrame(
        {
            "subject_id": [1, 1],
            "time": [None, None],
            "code": ["A", "B"],
            "numeric_value": [1.0, 2.0],
            "text_value": ["not copied", "not copied either"],
            "source_block": ["note/note", "drug/drug"],
            "link_id": [999, None],
        },
        schema_overrides={"time": pl.Datetime("us"), "link_id": pl.Int64},
    ).with_columns(code_components).write_parquet(data_dir / "train" / "0.parquet")
    pl.DataFrame(
        {"subject_id": [2], "code": ["C"], "source_block": ["note/note"]}
    ).write_parquet(data_dir / "held_out" / "0.parquet")

    written = build_meds_extract_linked_data(meds_dir)

    assert sorted(
        fp.relative_to(meds_dir / "metadata" / "meds_extract_data") for fp in written
    ) == [
        Path("held_out/0.parquet"),
        Path("train/0.parquet"),
    ]

    out = pl.read_parquet(
        meds_dir / "metadata" / "meds_extract_data" / "train" / "0.parquet"
    )
    assert out.columns == [
        "meds_extract_link_id",
        "meds_extract_row_idx",
        "source_block",
        "code_components",
    ]
    assert out.height == 2
    assert out["meds_extract_row_idx"].to_list() == [0, 1]
    assert out["source_block"].to_list() == ["note/note", "drug/drug"]
    assert out["meds_extract_link_id"].dtype == pl.Int64
    assert "text_value" not in out.columns
    assert "numeric_value" not in out.columns

    main = pl.read_parquet(data_dir / "train" / "0.parquet")
    assert "source_block" not in main.columns
    assert "code_components" not in main.columns
    assert "meds_extract_link_id" in main.columns
    assert (
        main["meds_extract_link_id"].to_list() == out["meds_extract_link_id"].to_list()
    )
    assert "link_id" in main.columns


def test_build_meds_extract_linked_data_refuses_overwrite(tmp_path: Path):
    meds_dir = tmp_path / "MEDS_cohort"
    data_dir = meds_dir / "data" / "train"
    data_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "subject_id": [1],
            "code": ["A"],
            "link_id": [2],
            "source_block": ["note/note"],
            "code_components": [{"preferred_concept_name": "1"}],
        },
        schema_overrides={
            "code_components": pl.Struct({"preferred_concept_name": pl.Utf8})
        },
    ).write_parquet(data_dir / "0.parquet")

    build_meds_extract_linked_data(meds_dir)

    with pytest.raises(FileExistsError):
        build_meds_extract_linked_data(meds_dir)
