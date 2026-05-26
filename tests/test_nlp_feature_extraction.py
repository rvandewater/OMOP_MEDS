from pathlib import Path
import polars as pl
from omop_schema.utils import get_schema_loader
from OMOP_MEDS.pre_meds_utils import (
    calculate_nlp_features,
    extract_nlp_features,
    get_patient_link,
    load_raw_file,
)

DEMO_DIR = Path(__file__).resolve().parent / "demo_resources"


def test_extract_nlp_features_from_demo_notes():
    schema_loader = get_schema_loader(5.3)
    person_df = load_raw_file(DEMO_DIR / "person.csv", schema_loader)
    death_df = load_raw_file(DEMO_DIR / "death.csv", schema_loader)
    visit_df = load_raw_file(DEMO_DIR / "visit_occurrence.csv", schema_loader)
    note_df = pl.scan_csv(DEMO_DIR / "note.csv")
    patient_link = get_patient_link(
        person_df=person_df,
        death_df=death_df,
        visit_df=visit_df,
        schema_loader=schema_loader,
    )
    fn = extract_nlp_features(
        table_name="note",
        text_column="note_text",
        features=["word_count", "char_count", "lexical_diversity"],
        prefix="note",
        output_data_cols=["note_id", "note_date", "note_type_concept_id"],
    )
    out = fn(note_df, patient_link).collect().sort("note_id")
    assert out.height == 3
    assert out["person_id"].to_list() == [
        3589912774911670296,
        -775517641933593374,
        3912882389848878631,
    ]
    assert out["note_feature_word_count"].to_list() == [3, 4, 5]
    assert out["note_feature_char_count"].to_list() == [25, 20, 21]
    assert out["note_feature_lexical_diversity"].to_list() == [1.0, 1.0, 1.0]
    assert "note_text" not in out.columns


def test_calculate_nlp_features_handles_empty_and_none_text():
    assert calculate_nlp_features(
        None, features=["word_count", "lexical_diversity"]
    ) == {
        "feature_word_count": 0,
        "feature_lexical_diversity": 0.0,
    }
    assert calculate_nlp_features("", features=["char_count"]) == {
        "feature_char_count": 0
    }
