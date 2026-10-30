"""Code metadata: every MEDS code is described by the OMOP concept it was built from.

All vocabularies, codes and names below are synthetic.
"""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema

from OMOP_MEDS import EVENT_CFG
from OMOP_MEDS.pre_meds_utils import extract_codes_metadata, join_concept
from OMOP_MEDS.utils import enrich_codes_metadata_with_athena, finish_codes_metadata

# Concept 10's concept_code is "20", and concept 20 exists in the same vocabulary: a code keyed by
# concept_code ("VOC//20" from measurement) and one keyed by concept_id ("VOC//20//start" from drug_era)
# share their concept component but stand for different concepts.
CONCEPT = pl.DataFrame(
    {
        "concept_id": [10, 20, 30, 40, 2000000001],
        "concept_name": [
            "Lab ten",
            "Drug twenty",
            "Standard thirty",
            "Drug forty",
            "Local lab",
        ],
        "vocabulary_id": ["VOC", "VOC", "STD", "VOC", "LOCAL"],
        "concept_code": ["20", "D20", "S30", "D40", "L1"],
    }
)
# Targets 31 and 50 are not in the source concept table.
CONCEPT_RELATIONSHIP = pl.DataFrame(
    {
        "concept_id_1": [10, 10, 30, 2000000001],
        "concept_id_2": [30, 31, 30, 50],
        "relationship_id": ["Maps to"] * 4,
    }
)


def _write_dataset(root: Path) -> tuple[Path, Path]:
    pre_MEDS_dir, MEDS_cohort_dir = root / "pre_MEDS", root / "MEDS_cohort"
    pre_MEDS_dir.mkdir()
    CONCEPT.write_parquet(pre_MEDS_dir / "concept.parquet")
    CONCEPT_RELATIONSHIP.write_parquet(pre_MEDS_dir / "concept_relationship.parquet")
    extract_codes_metadata(CONCEPT.lazy(), CONCEPT_RELATIONSHIP.lazy()).sink_parquet(
        pre_MEDS_dir / "codes.parquet"
    )
    # Pre-MEDS tables as join_concept writes them: measurement has a mapped and a source concept,
    # so `preferred_concept_name` is a concept_code; drug_era has one, so it is a concept_id.
    pl.DataFrame(
        {
            "preferred_vocabulary_name": ["VOC", "LOCAL", "OMOP_measurement"],
            "preferred_concept_name": ["20", "L1", "measurement_concept_id:60,61"],
            "preferred_concept_id": [10, 2000000001, 60],
            "table_name": ["measurement"] * 3,
        }
    ).write_parquet(pre_MEDS_dir / "measurement.parquet")
    pl.DataFrame(
        {
            "preferred_vocabulary_name": ["VOC"],
            "preferred_concept_name": [20],
            "preferred_concept_id": [20],
            "table_name": ["drug_era"],
        }
    ).write_parquet(pre_MEDS_dir / "drug_era.parquet")
    pl.DataFrame(
        {
            "preferred_vocabulary_name": ["VOC"],
            "drug_concept_id": [40],
            "preferred_concept_name": ["D40"],
            "preferred_concept_id": [40],
            "table_name": ["drug_exposure"],
        }
    ).write_parquet(pre_MEDS_dir / "drug_exposure.parquet")

    (MEDS_cohort_dir / "data/train").mkdir(parents=True)
    pl.DataFrame(
        {
            "subject_id": [1, 1, 1, 1, 1, 1, 2, 2],
            "code": [
                "VOC//20",
                "LOCAL//L1",
                "OMOP_measurement//measurement_concept_id:60,61",
                "VOC//20//start",
                "VOC//20//end",
                "VOC//40//start",
                "VOC//20",
                "MEDS_BIRTH",
            ],
            "table_name": [
                "measurement",
                "measurement",
                "measurement",
                "drug_era",
                "drug_era",
                "drug_exposure",
                "measurement",
                "person_death",
            ],
        }
    ).write_parquet(MEDS_cohort_dir / "data/train/0.parquet")
    return pre_MEDS_dir, MEDS_cohort_dir


def _rows(codes: pl.DataFrame) -> dict[tuple[str, str], dict]:
    return {(r["code"], r["table_name"]): r for r in codes.iter_rows(named=True)}


def test_codes_are_described_by_the_concept_they_were_built_from(tmp_path):
    pre_MEDS_dir, MEDS_cohort_dir = _write_dataset(tmp_path)

    codes_fp = finish_codes_metadata(MEDS_cohort_dir, pre_MEDS_dir, EVENT_CFG)

    table = pq.read_table(codes_fp)
    CodeMetadataSchema.validate(table)
    codes = pl.from_arrow(table)
    assert codes.height == codes.select("code", "table_name").n_unique() == 7
    rows = _rows(codes)

    # concept_code-keyed code: concept 10, not concept 20 whose concept_id equals the code component.
    lab = rows["VOC//20", "measurement"]
    assert (lab["concept_id"], lab["description"]) == (10, "Lab ten")
    assert lab["occurrence_count"] == 2
    # Every "Maps to" target in the concept table, in one list; the missing target 31 is skipped.
    assert lab["parent_codes"] == ["STD//S30"]
    # concept_id-keyed codes (drug_era, drug_exposure's raw drug_concept_id): concept 20 and 40.
    for suffix in ("//start", "//end"):
        drug = rows[f"VOC//20{suffix}", "drug_era"]
        assert (drug["concept_id"], drug["description"]) == (20, "Drug twenty")
    assert rows["VOC//40//start", "drug_exposure"]["description"] == "Drug forty"
    # A custom concept whose only "Maps to" target is missing from the source concept table.
    local = rows["LOCAL//L1", "measurement"]
    assert (local["description"], local["parent_codes"]) == ("Local lab", None)
    # A fallback code is linked to the concept its column names, even when it is not in the table.
    fallback = rows["OMOP_measurement//measurement_concept_id:60,61", "measurement"]
    assert (fallback["concept_id"], fallback["description"]) == (60, None)
    assert rows["MEDS_BIRTH", "person_death"]["concept_id"] is None


def test_a_standard_code_is_not_its_own_parent(tmp_path):
    pre_MEDS_dir, MEDS_cohort_dir = _write_dataset(tmp_path)
    pl.DataFrame(
        {
            "preferred_vocabulary_name": ["STD"],
            "preferred_concept_name": ["S30"],
            "preferred_concept_id": [30],
            "table_name": ["measurement"],
        }
    ).write_parquet(pre_MEDS_dir / "measurement.parquet")
    pl.DataFrame(
        {"subject_id": [1], "code": ["STD//S30"], "table_name": ["measurement"]}
    ).write_parquet(MEDS_cohort_dir / "data/train/0.parquet")

    codes = pl.read_parquet(
        finish_codes_metadata(MEDS_cohort_dir, pre_MEDS_dir, EVENT_CFG)
    )

    assert codes.select("description", "parent_codes").row(0) == (
        "Standard thirty",
        None,
    )


@pytest.mark.parametrize(
    "mapped_id, source_id, prefer_source, expected",
    [
        (10, 2000000001, False, ("VOC", "20", 10)),
        (10, 2000000001, True, ("LOCAL", "L1", 2000000001)),
        (99, 2000000001, False, ("LOCAL", "L1", 2000000001)),
        (99, 98, False, (None, "measurement_concept_id:99,98", 99)),
    ],
)
def test_join_concept_records_the_concept_behind_the_code(
    mapped_id, source_id, prefer_source, expected
):
    func = join_concept(
        table_name="measurement",
        reference_cols=["measurement_concept_id", "measurement_source_concept_id"],
        concept_cols=["vocabulary_id", "concept_code"],
        prefer_source=prefer_source,
    )
    df = pl.LazyFrame(
        {
            "person_id": [1],
            "measurement_concept_id": [mapped_id],
            "measurement_source_concept_id": [source_id],
        }
    )

    out = func(df, CONCEPT.lazy(), pl.LazyFrame({"person_id": [1]})).collect()

    vocabulary, concept_name, concept_id = expected
    assert out["preferred_concept_name"].item() == concept_name
    assert out["preferred_concept_id"].item() == concept_id
    if vocabulary is not None:
        assert out["preferred_vocabulary_name"].item() == vocabulary


def _write_athena(athena_dir: Path) -> None:
    """A synthetic Athena download: tab-separated, unquoted, like OHDSI ships it."""
    athena_dir.mkdir()
    pl.DataFrame(
        {
            "concept_id": [10, 30, 31, 50, 60],
            "concept_name": [
                "Athena name for ten",
                "Athena standard thirty",
                "Athena standard thirty-one",
                "Athena standard fifty",
                "Athena lab sixty",
            ],
            "domain_id": ["Measurement"] * 5,
            "vocabulary_id": ["VOC", "STD", "STD", "STD", "STD"],
            "concept_class_id": ["Lab Test"] * 5,
            "standard_concept": [None, "S", "S", "S", "S"],
            "concept_code": ["20", "S30", "S31", "S50", "S60"],
            "valid_start_date": ["19700101"] * 5,
            "valid_end_date": ["20991231"] * 5,
            "invalid_reason": [None] * 5,
        }
    ).write_csv(athena_dir / "CONCEPT.csv", separator="\t", quote_style="never")
    pl.DataFrame(
        {
            "concept_id_1": [60, 10],
            "concept_id_2": [60, 31],
            "relationship_id": ["Maps to", "Maps to"],
            "valid_start_date": ["19700101"] * 2,
            "valid_end_date": ["20991231"] * 2,
            "invalid_reason": [None, None],
        }
    ).write_csv(
        athena_dir / "CONCEPT_RELATIONSHIP.csv", separator="\t", quote_style="never"
    )


def test_athena_fallback_only_fills_gaps(tmp_path):
    pytest.importorskip("sumeds")
    pre_MEDS_dir, MEDS_cohort_dir = _write_dataset(tmp_path)
    codes_fp = finish_codes_metadata(MEDS_cohort_dir, pre_MEDS_dir, EVENT_CFG)
    before = _rows(pl.read_parquet(codes_fp))
    athena_dir = tmp_path / "athena"
    _write_athena(athena_dir)

    enrich_codes_metadata_with_athena(codes_fp, pre_MEDS_dir, athena_dir)

    table = pq.read_table(codes_fp)
    CodeMetadataSchema.validate(table)
    after = _rows(pl.from_arrow(table))
    assert after.keys() == before.keys()
    for key, row in before.items():
        for column in ("description", "parent_codes", "vocabulary_id", "concept_code"):
            if row[column] is not None:
                assert after[key][column] == row[column], (key, column)
    # The source "Maps to" target missing from the source concept table is resolved through Athena.
    assert after["LOCAL//L1", "measurement"]["parent_codes"] == ["STD//S50"]
    # "VOC//20//start" stands for concept 20, which Athena lacks. Matched as a VOCABULARY//CODE string it
    # would hit concept 10 (concept_code "20") and get its Athena "Maps to" parent.
    assert after["VOC//20//start", "drug_era"]["parent_codes"] is None
    # A concept missing from the source tables is described and mapped through Athena.
    fallback = after["OMOP_measurement//measurement_concept_id:60,61", "measurement"]
    assert fallback["description"] == "Athena lab sixty"
    assert fallback["parent_codes"] == ["STD//S60"]
    assert (fallback["vocabulary_id"], fallback["concept_code"]) == ("STD", "S60")
    # Literal codes stay as they are.
    assert after["MEDS_BIRTH", "person_death"]["description"] is None
