"""Categorical results (`value_as_concept_id`) and units (`unit_concept_id`) reach MEDS as strings.

`concept_lookup_cols` adds a concept's `concept_name` or `concept_code`; the event configs use the name of a
categorical result as `text_value` and the UCUM code of a unit, else its source value, as `unit`.
"""

from pathlib import Path

import polars as pl
import pytest
from omop_run import run_omop
from test_omop_versions import CDM, CONFIGS, _stage_demo_as

from OMOP_MEDS.omop_versions import SUPPORTED_OMOP_VERSIONS
from OMOP_MEDS.pre_meds_utils import join_concept

# Synthetic concepts, in the column layout of the demo's concept.csv.
CONCEPTS = pl.DataFrame(
    [
        (8840, "milligram per deciliter", "Unit", "UCUM", "Unit", "mg/dL"),
        (8587, "milliliter", "Unit", "UCUM", "Unit", "mL"),
        (9191, "Positive", "Meas Value", "SNOMED", "Qualifier Value", "10828004"),
        (9189, "Negative", "Meas Value", "SNOMED", "Qualifier Value", "260385009"),
        (45879404, "Never smoker", "Meas Value", "LOINC", "Answer", "LA18978-9"),
        (3013682, "Urea nitrogen", "Measurement", "LOINC", "Lab Test", "3094-0"),
    ],
    schema=[
        "concept_id",
        "concept_name",
        "domain_id",
        "vocabulary_id",
        "concept_class_id",
        "concept_code",
    ],
    orient="row",
).with_columns(
    standard_concept=pl.lit("S"),
    valid_start_DATE=pl.lit("1970-01-01"),
    valid_end_DATE=pl.lit("2099-12-31"),
    invalid_reason=pl.lit(None, pl.String),
)
# A known concept and the field a lookup gives for it, per field; and a concept missing from the vocabulary.
KNOWN = {"concept_code": (8840, "mg/dL"), "concept_name": (9191, "Positive")}
UNKNOWN = 999_999_999


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_concept_lookups_of_each_versions_blocks(version):
    lookups = {
        table: block["concept_lookup_cols"]
        for table, block in CONFIGS[version].items()
        if block.get("concept_lookup_cols")
    }
    # device_exposure has no unit columns in 5.3, and its event config is shared by every version.
    assert lookups == {
        "observation": {
            "unit_concept_id": "concept_code",
            "value_as_concept_id": "concept_name",
        },
        "measurement": {
            "unit_concept_id": "concept_code",
            "value_as_concept_id": "concept_name",
        },
        "specimen": {"unit_concept_id": "concept_code"},
        "dose_era": {"unit_concept_id": "concept_code"},
    }
    concept_df = CONCEPTS.lazy().with_columns(pl.col("concept_id").cast(pl.Int64))
    person_df = pl.LazyFrame({"person_id": [1]}, schema={"person_id": pl.Int64})

    for table, table_lookups in lookups.items():
        block = CONFIGS[version][table]
        # Concept known, 0 (no concept), null and missing from the vocabulary.
        df = pl.LazyFrame(
            {"person_id": [1] * 4}
            | dict.fromkeys(block["reference_cols"], [None] * 4)
            | {
                col: [KNOWN[field][0], 0, None, UNKNOWN]
                for col, field in table_lookups.items()
            },
            schema_overrides=dict.fromkeys(
                [*block["reference_cols"], *table_lookups], pl.Int64
            ),
        )
        out = join_concept(table, **block)(df, concept_df, person_df).collect()

        for col, field in table_lookups.items():
            name = f"{col.removesuffix('_concept_id')}_{field}"
            assert out.schema[name] == pl.String
            assert out[name].to_list() == [KNOWN[field][1], None, None, None], name

        # A lookup column missing from the input gives a null output column.
        missing = next(iter(table_lookups))
        out = join_concept(table, **block)(
            df.drop(missing), concept_df, person_df
        ).collect()
        name = f"{missing.removesuffix('_concept_id')}_{table_lookups[missing]}"
        assert out[name].null_count() == out.height == 4


def _stage_categorical_values_and_units(raw: Path) -> None:
    """Adds synthetic concepts, measurements and one categorical observation to staged demo resources."""
    concepts = pl.read_csv(raw / "concept.csv", infer_schema=False)
    pl.concat([concepts, CONCEPTS.cast(pl.String).select(concepts.columns)]).write_csv(
        raw / "concept.csv"
    )

    visit = pl.read_csv(raw / "visit_occurrence.csv", infer_schema=False).row(
        0, named=True
    )
    date = visit["visit_start_date"]
    # Categorical results without a number or string, and units by UCUM concept or by source value only. The
    # first two rows differ only in their result.
    known = pl.DataFrame(
        {
            "measurement_id": ["1", "2", "3", "4"],
            "measurement_concept_id": ["3013682"] * 4,
            "measurement_date": [date] * 4,
            "measurement_datetime": [f"{date} 08:00:00"] * 4,
            "measurement_type_concept_id": ["32817"] * 4,
            "value_as_number": [None, None, "5.4", "120"],
            "value_as_concept_id": ["9191", "9189", None, "0"],
            "unit_concept_id": [None, None, "8840", "0"],
            "unit_source_value": [None, None, "mg/dl", "mmHg"],
            "visit_occurrence_id": [visit["visit_occurrence_id"]] * 4,
            "person_id": [visit["person_id"]] * 4,
        }
    )
    # Like an OMOP export, with every CDM column, empty where unknown.
    known.select(
        pl.col(c) if c in known.columns else pl.lit(None, pl.String).alias(c)
        for c in CDM["5.3"]["measurement"]
    ).write_csv(raw / "measurement.csv")

    observations = pl.read_csv(raw / "observation.csv", infer_schema=False)
    bare = pl.all_horizontal(
        pl.col("value_as_number", "value_as_string", "value_as_concept_id").is_null()
    )
    first_bare = (
        pl.int_range(pl.len()) == observations.select(bare.arg_true().first()).item()
    )
    # Only a categorical result, and a unit by its source value only.
    changes = {
        "value_as_concept_id": "45879404",
        "unit_concept_id": "0",
        "unit_source_value": "kg",
    }
    observations.with_columns(
        pl.when(first_bare).then(pl.lit(v)).otherwise(c).alias(c)
        for c, v in changes.items()
    ).write_csv(raw / "observation.csv")

    # A single CSV file without `unit_source_value`, which the event config falls back to.
    specimens = pl.read_csv(raw / "specimen.csv", infer_schema=False)
    specimens.drop("unit_source_value").with_columns(
        unit_concept_id=pl.when(pl.int_range(pl.len()) == 0).then(pl.lit("8587"))
    ).write_csv(raw / "specimen.csv")


def test_categorical_values_and_unit_strings_reach_meds(tmp_path: Path, monkeypatch):
    raw = _stage_demo_as("5.3", tmp_path / "raw")
    _stage_categorical_values_and_units(raw)
    monkeypatch.setenv("OMOP_VERSION", "5.3")

    run_omop(tmp_path / "out", raw, "skip_stages=[sumeds_enrich]")

    events = pl.concat(
        [
            pl.read_parquet(fp)
            for fp in sorted((tmp_path / "out" / "data").rglob("*.parquet"))
        ]
    )

    def values(table: str, col: str) -> list:
        rows = events.filter(pl.col("table_name") == table)
        return sorted(rows[col].drop_nulls().to_list())

    assert events.filter(pl.col("table_name") == "measurement").height == 4
    assert values("measurement", "text_value") == ["Negative", "Positive"]
    assert values("measurement", "unit") == ["mg/dL", "mmHg"]
    assert values("observation", "text_value").count("Never smoker") == 1
    assert values("observation", "unit") == ["kg"]
    assert values("specimen", "unit") == ["mL"]
    assert events.schema["unit"] == pl.String
    assert events.filter(pl.col("unit").str.contains(r"^\d+$")).height == 0
