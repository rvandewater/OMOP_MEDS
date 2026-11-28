"""Regression tests for code paths that differ between Polars versions.

The package requires Polars >= 2.1, which `pip install` and `uv.lock` both resolve; these tests keep the code
paths that changed in Polars 2 (and the streaming join slowdown before 2.1) from coming back.
"""

import time
from pathlib import Path

import polars as pl

import OMOP_MEDS
from OMOP_MEDS.pre_meds_utils import extract_codes_metadata, join_concept

SRC_DIR = Path(OMOP_MEDS.__file__).parent


def test_parent_codes_are_string_lists_or_null():
    # Polars 2 no longer casts String to List(String); the parent codes are aggregated into lists instead,
    # which must give List(String) on both. IDs are Int64, as the OMOP schema loader types them.
    concept = pl.LazyFrame(
        {
            "concept_id": [1, 2, 3],
            "vocabulary_id": ["ICD10", "SNOMED", "LOINC"],
            "concept_code": ["A01", "C2", "L3"],
            "concept_name": ["source", "standard", "unmapped"],
        }
    )
    concept_relationship = pl.LazyFrame(
        {
            "concept_id_1": [1, 1, 3],
            "concept_id_2": [2, 3, 2],
            "relationship_id": ["Maps to", "Is a", "Mapped from"],
        }
    )

    out = (
        extract_codes_metadata(concept, concept_relationship)
        .collect()
        .sort("concept_id")
    )

    assert out.schema["parent_codes"] == pl.List(pl.String)
    assert out.select("code", "parent_codes").rows() == [
        ("ICD10//1", ["SNOMED//C2"]),
        ("SNOMED//2", None),
        ("LOINC//3", None),
    ]


def test_no_imports_from_modules_removed_in_polars_2():
    # Only a DeprecationWarning on Polars 1.x, so this would otherwise pass locally and fail on Polars 2.
    offenders = [
        str(fp.relative_to(SRC_DIR))
        for fp in SRC_DIR.rglob("*.py")
        if "polars.type_aliases" in fp.read_text()
    ]
    assert offenders == []


def _sink_seconds(
    no_source_concepts: bool, tmp_path: Path, n_rows: int = 40_000
) -> float:
    """Best-of-3 seconds to sink the pre-MEDS concept join of a synthetic visit_detail table.

    With `no_source_concepts`, every `visit_detail_source_concept_id` is 0, as in exports without source
    concepts; pre-MEDS turns 0 into null before the concept join, so that join's key column is entirely null.
    """
    row = pl.int_range(n_rows, eager=True)
    pl.DataFrame(
        {
            "person_id": row % 1_000,
            "visit_detail_id": row,
            "visit_detail_concept_id": row % 1_000 + 1,
            "visit_detail_source_concept_id": row * 0
            if no_source_concepts
            else row % 1_000 + 1_001,
        }
    ).write_parquet(tmp_path / "visit_detail.parquet")
    ids = pl.int_range(1, 1_000_001, eager=True)
    concept = pl.DataFrame(
        {
            "concept_id": ids,
            "vocabulary_id": pl.repeat("V", len(ids), eager=True),
            "concept_code": ids.cast(pl.String),
        }
    ).lazy()
    person = pl.LazyFrame({"person_id": pl.int_range(1_000, eager=True)})
    fn = join_concept(
        "visit_detail",
        ["visit_detail_concept_id", "visit_detail_source_concept_id"],
        ["visit_detail_id"],
        [
            "vocabulary_id",
            "concept_code",
            "vocabulary_id_source_concept_id",
            "concept_code_source_concept_id",
        ],
    )
    timings = []
    for _ in range(3):
        out = fn(pl.scan_parquet(tmp_path / "visit_detail.parquet"), concept, person)
        start = time.perf_counter()
        out.sink_parquet(tmp_path / "out.parquet")
        timings.append(time.perf_counter() - start)
        assert (
            pl.scan_parquet(tmp_path / "out.parquet").select(pl.len()).collect().item()
            == n_rows
        )
    return min(timings)


def test_streaming_concept_join_on_an_all_null_key_is_not_pathologically_slow(tmp_path):
    # Polars < 2.1 streams a left join whose key column is entirely null in time that grows faster than linearly
    # with the rows: INSPECT's visit_detail (1M rows, source concept ids all 0) took more than 20 minutes to
    # write on 1.41.2, and 0.2 s on 2.1. On 40k rows that is about 6 s on 1.41 and 0.4 s on 2.0, against
    # milliseconds with matching keys; the margin is wide so that only that slowdown, not runner noise, fails.
    all_null = _sink_seconds(True, tmp_path)
    matching = _sink_seconds(False, tmp_path)

    assert all_null < 5 * matching + 0.1, (
        f"all-null key {all_null:.2f}s, matching key {matching:.2f}s"
    )
