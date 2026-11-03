"""Branch coverage for `pre_meds_utils` helpers and `utils.finish_codes_metadata` on tiny synthetic data.

Each test pins one code path that the end-to-end runs over the demo resources never take: a table stored as a
directory, persons without a death table or birth datetime, concept joins with default or string arguments,
re-running metadata setup over existing outputs, and so on.
"""

import datetime as dt
import logging
import shutil
from pathlib import Path

import polars as pl
import polars.selectors as cs
import pytest
from omop_schema.utils import get_schema_loader

from OMOP_MEDS.pre_meds_utils import (
    SUBJECT_ID,
    build_preferred_event_datetime,
    calculate_nlp_features,
    cast_files_to_schema,
    check_column_mismatches,
    col_selector,
    extract_nlp_features,
    get_patient_link,
    get_table_path,
    join_concept,
    rename_demo_files,
    set_up_metadata,
)
from OMOP_MEDS.utils import finish_codes_metadata

DEMO_RESOURCES_DIR = Path(__file__).resolve().parent / "demo_resources"
SCHEMA_LOADER = get_schema_loader(5.3)
ALL_NLP_FEATURES = [
    "word_count",
    "char_count",
    "sentence_count",
    "avg_word_length",
    "avg_sentence_length",
    "punctuation_count",
    "digit_count",
    "uppercase_count",
    "unique_word_count",
    "lexical_diversity",
]


# --- get_table_path / build_preferred_event_datetime ------------------------------------------------------


def test_get_table_path_resolves_directories_files_and_missing_tables(tmp_path: Path):
    (tmp_path / "measurement").mkdir()
    (tmp_path / "person.csv.gz").write_bytes(b"")

    assert get_table_path(tmp_path, "measurement") == tmp_path / "measurement"
    assert get_table_path(tmp_path, "person") == tmp_path / "person.csv.gz"
    assert get_table_path(tmp_path, "death") is None


def test_preferred_datetime_without_override_or_usable_column_is_a_typed_null():
    schema = pl.Schema({"note_id": pl.Int64})

    expr = build_preferred_event_datetime(
        schema,
        primary_datetime_col="note_datetime",
        primary_date_col="note_date",
        use_override_if_later=False,
    )
    out = pl.DataFrame({"note_id": [1, 2]}).with_columns(expr)

    assert out.columns == ["note_id", "preferred_time"]
    assert out.schema["preferred_time"] == pl.Datetime("us")
    assert out["preferred_time"].null_count() == 2


# --- get_patient_link -------------------------------------------------------------------------------------


def _persons(with_gender: bool = True) -> pl.LazyFrame:
    data = {
        SUBJECT_ID: [1, 2, 3],
        "year_of_birth": [1980, 1990, 2000],
        "month_of_birth": [1, 2, 3],
        "day_of_birth": [1, 2, 3],
    }
    if with_gender:
        data["gender_concept_id"] = [8507, 8532, 0]
    return pl.LazyFrame(data)


VISITS = pl.LazyFrame({SUBJECT_ID: [1, 2, 3]})


def _link(person_df: pl.LazyFrame, death_df: pl.LazyFrame | None, **kwargs):
    return (
        get_patient_link(person_df, death_df, VISITS, SCHEMA_LOADER, **kwargs)
        .collect()
        .sort(SUBJECT_ID)
    )


def test_patient_link_uses_gender_concept_and_birth_parts_and_death_date():
    # No gender_source_concept_id and no birth_datetime: gender comes from gender_concept_id and the birth
    # datetime is assembled from year/month/day. A date-only death table falls back to death_date.
    death = pl.LazyFrame({SUBJECT_ID: [2], "death_date": [dt.date(2020, 5, 1)]})

    out = _link(_persons(), death)

    assert out["gender"].to_list() == [8507, 8532, 0]
    assert out["date_of_birth"].to_list() == [
        dt.datetime(1980, 1, 1),
        dt.datetime(1990, 2, 2),
        dt.datetime(2000, 3, 3),
    ]
    assert out["date_of_death"].to_list() == [None, dt.datetime(2020, 5, 1), None]
    assert out["table_name"].unique().to_list() == ["person"]


def test_patient_link_without_death_table_has_typed_null_deaths():
    out = _link(_persons(), None)

    assert out.height == 3
    assert out.schema["date_of_death"] == pl.Datetime("us")
    assert out["date_of_death"].null_count() == 3


def test_patient_link_death_table_without_death_columns_yields_null_deaths():
    death = pl.LazyFrame({SUBJECT_ID: [2], "cause_source_value": ["x"]})

    out = _link(_persons(), death)

    assert out["date_of_death"].null_count() == 3


def test_patient_link_limit_and_missing_gender():
    out = _link(_persons(with_gender=False), None, limit=2)

    assert out[SUBJECT_ID].to_list() == [1, 2]
    assert out["gender"].null_count() == 2


# --- join_concept -----------------------------------------------------------------------------------------

CONCEPTS = pl.LazyFrame(
    {
        "concept_id": [10, 20],
        "vocabulary_id": ["LOINC", "SNOMED"],
        "concept_name": ["Heart rate", "Fever"],
    }
)
PERSONS = pl.LazyFrame({SUBJECT_ID: [1, 2]})


def test_join_concept_with_defaults_keeps_subject_and_table_name_only():
    df = pl.LazyFrame(
        {
            SUBJECT_ID: [1, 2, 3],
            "value_as_number": [1.0, 2.0, 3.0],
            "table_name": ["observation"] * 3,
        }
    )

    out = join_concept("observation")(df, CONCEPTS, PERSONS).collect().sort(SUBJECT_ID)

    # Person 3 is not in the person table, so it is dropped.
    assert out.columns == [SUBJECT_ID, "table_name"]
    assert out.rows() == [(1, "observation"), (2, "observation")]


def test_join_concept_with_a_single_reference_column_given_as_str():
    df = pl.LazyFrame(
        {
            SUBJECT_ID: [1, 2],
            "observation_concept_id": [10, 0],
            "value_as_number": [70.0, 38.5],
        }
    )

    fn = join_concept(
        "observation",
        "observation_concept_id",
        output_data_cols=["value_as_number"],
        concept_cols=["concept_name"],
    )
    out = fn(df, CONCEPTS, PERSONS).collect().sort(SUBJECT_ID)

    assert out.columns == [
        "value_as_number",
        "concept_name",
        SUBJECT_ID,
        "preferred_concept_name",
        "preferred_vocabulary_name",
    ]
    # Concept id 0 means "no concept": it is nulled, joins nothing, and the vocabulary falls back to the
    # table-specific OMOP_<table> name.
    assert out.select(
        "concept_name", "preferred_concept_name", "preferred_vocabulary_name"
    ).rows() == [
        ("Heart rate", 10, "LOINC"),
        (None, None, "OMOP_observation"),
    ]


# --- col_selector -----------------------------------------------------------------------------------------

WIDE = pl.DataFrame(
    {
        "person_id": [1],
        "visit_id": [2],
        "x_source_value": ["a"],
        "y_source_value": ["b"],
    }
)


def test_col_selector_requires_columns_or_patterns():
    with pytest.raises(ValueError, match="Provide at least one of"):
        col_selector()


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"columns": ["person_id", "visit_id"]}, ["person_id", "visit_id"]),
        ({"patterns": "_source_value$"}, ["x_source_value", "y_source_value"]),
        (
            {"columns": ["person_id"], "patterns": ["^x_", "^y_"]},
            ["person_id", "x_source_value", "y_source_value"],
        ),
    ],
    ids=["columns", "str_pattern", "columns_and_patterns"],
)
def test_col_selector_selects_by_name_and_pattern(kwargs, expected):
    assert WIDE.select(col_selector(**kwargs)).columns == expected


# --- check_column_mismatches / cast_files_to_schema -------------------------------------------------------


def _write_shards(folder: Path, consistent: bool) -> Path:
    folder.mkdir()
    pl.DataFrame({"id": [1, 2], "v": [1, 2], "extra": ["a", "b"]}).write_parquet(
        folder / "a.parquet"
    )
    pl.DataFrame({"id": [3], "v": [3] if consistent else ["3"]}).write_parquet(
        folder / "b.parquet"
    )
    return folder


def test_check_column_mismatches_reports_inconsistent_dtypes(tmp_path: Path, capsys):
    mismatched = check_column_mismatches(_write_shards(tmp_path / "in", False))

    assert {col: set(dtypes) for col, dtypes in mismatched.items()} == {
        "v": {pl.Int64, pl.String}
    }
    assert "Mismatched columns found:" in capsys.readouterr().out


def test_check_column_mismatches_on_consistent_shards(tmp_path: Path, capsys):
    assert check_column_mismatches(_write_shards(tmp_path / "in", True)) == {}
    assert "All schemas are consistent" in capsys.readouterr().out


def test_cast_files_to_schema_aligns_every_shard(tmp_path: Path, capsys):
    src = _write_shards(tmp_path / "in", False)
    target = {"id": pl.Int64, "v": pl.Float64, "w": pl.String}

    cast_files_to_schema(str(src), target, str(tmp_path / "out"))

    out = {fp.name: pl.read_parquet(fp) for fp in sorted((tmp_path / "out").iterdir())}
    assert sorted(out) == ["a.parquet", "b.parquet"]
    for df in out.values():
        # Extra columns are dropped, missing ones added as nulls, mismatched ones cast.
        assert df.schema == pl.Schema(target)
        assert df["w"].null_count() == df.height
    assert out["a.parquet"]["v"].to_list() == [1.0, 2.0]
    assert out["b.parquet"]["v"].to_list() == [3.0]
    assert "Found mismatched columns" in capsys.readouterr().out


# --- rename_demo_files ------------------------------------------------------------------------------------


def test_rename_demo_files_strips_the_2b_prefix_only(tmp_path: Path):
    for name in ["2b_person.csv", "2b_visit_occurrence.csv", "concept.csv"]:
        (tmp_path / name).write_text("x\n")

    rename_demo_files(tmp_path)

    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "concept.csv",
        "person.csv",
        "visit_occurrence.csv",
    ]


# --- set_up_metadata --------------------------------------------------------------------------------------


def _stage_omop(raw: Path, tables=("concept", "person", "death", "visit_occurrence")):
    """Copy demo tables into `raw` and add a concept_relationship mapping each concept to itself."""
    raw.mkdir(parents=True)
    for table in tables:
        shutil.copy2(DEMO_RESOURCES_DIR / f"{table}.csv", raw / f"{table}.csv")
    concept_ids = pl.read_csv(DEMO_RESOURCES_DIR / "concept.csv")["concept_id"]
    pl.DataFrame(
        {
            "concept_id_1": concept_ids,
            "concept_id_2": concept_ids,
            "relationship_id": "Maps to",
            "valid_start_date": "1970-01-01",
            "valid_end_date": "2099-12-31",
            "invalid_reason": pl.Series([None] * len(concept_ids), dtype=pl.String),
        }
    ).write_csv(raw / "concept_relationship.csv")
    return raw


def _set_up(raw: Path, out: Path, do_overwrite: bool, join_on_visit: bool = True):
    out.mkdir(parents=True, exist_ok=True)
    return set_up_metadata(
        MEDS_input_dir=out,
        do_overwrite=do_overwrite,
        OMOP_input_dir=raw,
        join_on_visit=join_on_visit,
        limit=0,
        schema_loader=SCHEMA_LOADER,
        selector=cs.all(),
    )


METADATA_FILES = [
    "codes.parquet",
    "concept.parquet",
    "concept_relationship.parquet",
    "person_birth_death.parquet",
]


def _persons_with_visits() -> int:
    visits = pl.read_csv(
        DEMO_RESOURCES_DIR / "visit_occurrence.csv", infer_schema=False
    )
    persons = pl.read_csv(DEMO_RESOURCES_DIR / "person.csv", infer_schema=False)
    return persons.join(visits.select(SUBJECT_ID), on=SUBJECT_ID, how="semi")[
        SUBJECT_ID
    ].n_unique()


def _plant_sentinels(out: Path) -> None:
    """Replace each metadata output with a recognisably different file.

    concept: 1 row; person: 2 rows; codes: 3 rows; concept_relationship: 4 rows with no "Maps to" link, so
    codes rebuilt from it carry no parent codes (the staged CSV maps every concept to itself).
    """
    pl.read_parquet(out / "concept.parquet").head(1).write_parquet(
        out / "concept.parquet"
    )
    pl.read_parquet(out / "person_birth_death.parquet").head(2).write_parquet(
        out / "person_birth_death.parquet"
    )
    pl.read_parquet(out / "codes.parquet").head(3).write_parquet(out / "codes.parquet")
    pl.read_parquet(out / "concept_relationship.parquet").head(4).with_columns(
        relationship_id=pl.lit("Is a")
    ).write_parquet(out / "concept_relationship.parquet")


def test_set_up_metadata_writes_then_reuses_existing_outputs(tmp_path: Path):
    raw = _stage_omop(tmp_path / "raw")
    out = tmp_path / "pre_MEDS"

    concept_df, patient_df = _set_up(raw, out, do_overwrite=False)

    assert sorted(p.name for p in out.iterdir()) == METADATA_FILES
    assert concept_df.collect().height == 3885
    assert patient_df.collect()[SUBJECT_ID].n_unique() == _persons_with_visits()
    assert pl.read_parquet(out / "codes.parquet")["parent_codes"].null_count() == 0

    _plant_sentinels(out)
    concept_df, patient_df = _set_up(raw, out, do_overwrite=False)

    assert concept_df.collect().height == 1
    assert patient_df.collect().height == 2
    assert pl.read_parquet(out / "codes.parquet").height == 3, "existing codes reused"

    # Missing codes are rebuilt, from the reused 1-row concept and link-free concept_relationship sentinels.
    (out / "codes.parquet").unlink()
    _set_up(raw, out, do_overwrite=False)

    codes = pl.read_parquet(out / "codes.parquet")
    assert codes.height == 1
    assert codes["parent_codes"].to_list() == [None]


def test_set_up_metadata_overwrite_rebuilds_every_output(tmp_path: Path):
    raw = _stage_omop(tmp_path / "raw")
    out = tmp_path / "pre_MEDS"
    _set_up(raw, out, do_overwrite=False)
    _plant_sentinels(out)

    concept_df, patient_df = _set_up(raw, out, do_overwrite=True)

    assert concept_df.collect().height == 3885
    assert patient_df.collect()[SUBJECT_ID].n_unique() == _persons_with_visits()
    assert pl.read_parquet(out / "concept_relationship.parquet").height == 3885
    assert pl.read_parquet(out / "codes.parquet").height > 3


def test_set_up_metadata_without_death_table_has_no_deaths(tmp_path: Path):
    raw = _stage_omop(
        tmp_path / "raw", tables=("concept", "person", "visit_occurrence")
    )

    _, patient_df = _set_up(raw, tmp_path / "pre_MEDS", do_overwrite=False)
    patients = patient_df.collect()

    assert patients.height == _persons_with_visits()
    assert patients["date_of_death"].null_count() == patients.height
    assert patients["table_name"].unique().to_list() == ["person_death"]


@pytest.mark.parametrize(
    ("tables", "drop_relationship", "message"),
    [
        ((), False, "No concept table"),
        (("concept",), False, "No person table"),
        (
            ("concept", "person", "visit_occurrence"),
            True,
            "No concept relationship table",
        ),
    ],
    ids=["concept", "person", "concept_relationship"],
)
def test_set_up_metadata_requires_core_tables(
    tmp_path: Path, tables, drop_relationship, message
):
    raw = _stage_omop(tmp_path / "raw", tables=tables)
    if drop_relationship:
        (raw / "concept_relationship.csv").unlink()

    with pytest.raises(FileNotFoundError, match=message):
        _set_up(raw, tmp_path / "pre_MEDS", do_overwrite=False)


# --- NLP features -----------------------------------------------------------------------------------------


def test_calculate_nlp_features_defaults_to_every_feature():
    features = calculate_nlp_features("Pain improved. BP 120/80!")

    assert list(features) == [f"feature_{f}" for f in ALL_NLP_FEATURES]
    assert features["feature_word_count"] == 4
    assert features["feature_sentence_count"] == 2
    assert features["feature_digit_count"] == 5
    assert features["feature_uppercase_count"] == 3


def test_extract_nlp_features_defaults_use_table_name_prefix_and_all_features():
    notes = pl.LazyFrame({SUBJECT_ID: [1, 2], "note_text": ["Short note.", ""]})

    out = extract_nlp_features("note", "note_text")(notes, PERSONS).collect()

    assert out.columns == [f"note_feature_{f}" for f in ALL_NLP_FEATURES] + [SUBJECT_ID]
    assert out.sort(SUBJECT_ID)["note_feature_word_count"].to_list() == [2, 0]


def test_extract_nlp_features_without_text_column_keeps_output_columns_only():
    df = pl.LazyFrame({SUBJECT_ID: [1, 2, 3], "value": [1, 2, 3], "other": [4, 5, 6]})

    fn = extract_nlp_features(
        "observation", "note_text", features=["word_count"], output_data_cols=["value"]
    )
    out = fn(df, PERSONS).collect().sort(SUBJECT_ID)

    assert out.columns == ["value", SUBJECT_ID]
    assert out.rows() == [(1, 1), (2, 2)]


# --- utils.finish_codes_metadata --------------------------------------------------------------------------


def test_finish_codes_metadata_without_pre_meds_codes_writes_nothing(
    tmp_path: Path, caplog
):
    cohort, pre_meds = tmp_path / "MEDS_cohort", tmp_path / "pre_MEDS"
    pre_meds.mkdir()

    with caplog.at_level(logging.WARNING, logger="OMOP_MEDS.utils"):
        finish_codes_metadata(cohort, pre_meds)

    assert not (cohort / "metadata" / "codes.parquet").exists()
    assert any("codes.parquet not found" in r.message for r in caplog.records)
