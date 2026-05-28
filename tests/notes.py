import subprocess
from tempfile import TemporaryDirectory

import polars as pl
from pathlib import Path


def create_demo_omop_with_notes(output_dir: Path):
    """Create a minimal OMOP dataset with notes for testing NLP feature extraction.

    Args:
        output_dir: Directory where CSV files will be created.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Person table
    person_df = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "gender_concept_id": [8507, 8532, 8507],  # Male, Female, Male
            "year_of_birth": [1980, 1975, 1990],
            "month_of_birth": [1, 2, 3],
            "day_of_birth": [1, 2, 3],
            "birth_datetime": [
                "1980-01-01 00:00:00",
                "1975-02-02 00:00:00",
                "1990-03-03 00:00:00",
            ],
            "race_concept_id": [8527, 8527, 8527],
            "ethnicity_concept_id": [38003564, 38003564, 38003564],
        }
    )
    person_df.write_csv(output_dir / "person.csv")

    # Visit occurrence table
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [101, 102, 103],
            "person_id": [1, 2, 3],
            "visit_concept_id": [9201, 9201, 9202],  # Inpatient, Inpatient, Outpatient
            "visit_start_date": ["2023-01-15", "2023-02-20", "2023-03-10"],
            "visit_end_date": ["2023-01-20", "2023-02-25", "2023-03-10"],
            "visit_start_datetime": [
                "2023-01-15 08:00:00",
                "2023-02-20 09:00:00",
                "2023-03-10 10:00:00",
            ],
            "visit_end_datetime": [
                "2023-01-20 12:00:00",
                "2023-02-25 11:30:00",
                "2023-03-10 10:30:00",
            ],
            "visit_source_concept_id": [0, 0, 0],
            "provider_id": [201, 202, 203],
            "care_site_id": [301, 302, 303],
            "care_site_name": ["Ward A", "Ward B", "Clinic C"],
        }
    )
    visit_df.write_csv(output_dir / "visit_occurrence.csv")

    # Note table with various text samples for NLP feature extraction
    note_df = pl.DataFrame(
        {
            "note_id": [1001, 1002, 1003, 1004, 1005],
            "person_id": [
                1,
                1,
                2,
                2,
                3,
            ],
            "visit_occurrence_id": [101, 101, 102, 102, 103],
            "note_date": [
                "2023-01-15",
                "2023-01-16",
                "2023-02-20",
                "2023-02-21",
                "2023-03-10",
            ],
            "note_datetime": [
                "2023-01-15 10:30:00",
                "2023-01-16 14:20:00",
                "2023-02-20 09:15:00",
                "2023-02-21 11:45:00",
                "2023-03-10 15:30:00",
            ],
            "note_type_concept_id": [44814637, 44814638, 44814637, 44814639, 44814637],
            "note_class_concept_id": [706391, 706391, 706391, 706391, 706391],
            "note_title": [
                "Chest Pain Follow-up",
                "Improving Symptoms",
                "Severe Headache",
                "Discharge Summary",
                "Annual Physical",
            ],
            "provider_id": [201, 202, 203, 204, 205],
            # Progress note, Discharge summary, etc.
            "note_text": [
                "Patient presents with chest pain. History of hypertension and diabetes mellitus type 2. Physical exam reveals elevated blood pressure 145/92. EKG shows normal sinus rhythm. Plan: Continue current medications, schedule follow-up in 2 weeks.",
                "Follow-up visit. Patient reports improved symptoms. Blood pressure controlled at 125/80. Continue current treatment regimen.",
                "Chief complaint: Severe headache for 3 days. Associated symptoms include nausea, photophobia. Neurological exam within normal limits. CT scan ordered. Diagnosis: Migraine without aura. Treatment: Prescribed sumatriptan 50mg PRN.",
                "Discharge summary: Patient admitted with community-acquired pneumonia. Treated with IV antibiotics (ceftriaxone 1g daily). Clinical improvement noted. Afebrile for 48 hours. Discharge medications: Amoxicillin 500mg TID for 7 days.",
                "Annual physical examination. Patient denies any complaints. Vitals stable. Lab results pending.",
            ],
            "encoding_concept_id": [32678, 32678, 32678, 32678, 32678],  # UTF-8
            "language_concept_id": [4180186, 4180186, 4180186, 4180186, 4180186],
            "visit_detail_id": [None, None, None, None, None],
            "note_source_value": ["EHR", "EHR", "EHR", "EHR", "EHR"],
        }
    )
    note_df.write_csv(output_dir / "note.csv")

    # Observation table (optional, for additional context)
    observation_df = pl.DataFrame(
        {
            "observation_id": [2001, 2002, 2003],
            "person_id": [1, 2, 3],
            "observation_concept_id": [4298794, 4298794, 4298794],
            "observation_date": ["2023-01-15", "2023-02-20", "2023-03-10"],
            "observation_datetime": [
                "2023-01-15 08:00:00",
                "2023-02-20 09:00:00",
                "2023-03-10 10:00:00",
            ],
            "observation_type_concept_id": [38000280, 38000280, 38000280],
            "value_as_number": [145.0, 180.0, 120.0],
            "value_as_string": [None, None, None],
            "value_as_concept_id": [None, None, None],
            "qualifier_concept_id": [None, None, None],
            "unit_concept_id": [0, 0, 0],
            "visit_occurrence_id": [101, 102, 103],
            "visit_detail_id": [None, None, None],
            "observation_source_value": ["BP", "BP", "BP"],
            "observation_source_concept_id": [0, 0, 0],
        }
    )
    observation_df.write_csv(output_dir / "observation.csv")

    # Concept table with relevant concepts
    concept_df = pl.DataFrame(
        {
            "concept_id": [
                8507,
                8532,
                9201,
                9202,
                44814637,
                44814638,
                44814639,
                4298794,
                32678,
            ],
            "concept_name": [
                "Male",
                "Female",
                "Inpatient Visit",
                "Outpatient Visit",
                "Progress note",
                "Discharge summary",
                "History and physical note",
                "Blood pressure",
                "UTF-8",
            ],
            "domain_id": [
                "Gender",
                "Gender",
                "Visit",
                "Visit",
                "Note Type",
                "Note Type",
                "Note Type",
                "Observation",
                "Metadata",
            ],
            "vocabulary_id": [
                "Gender",
                "Gender",
                "Visit",
                "Visit",
                "Note Type",
                "Note Type",
                "Note Type",
                "LOINC",
                "OMOP generated",
            ],
            "concept_class_id": [
                "Gender",
                "Gender",
                "Visit",
                "Visit",
                "Note Type",
                "Note Type",
                "Note Type",
                "Clinical Observation",
                "Encoding",
            ],
            "concept_code": ["M", "F", "IP", "OP", "PN", "DS", "HP", "85354-9", "UTF8"],
        }
    )
    concept_df.write_csv(output_dir / "concept.csv")

    # Concept relationship table (minimal, enough for metadata generation)
    concept_relationship_df = pl.DataFrame(
        {
            "concept_id_1": [44814637, 44814638, 44814639],
            "concept_id_2": [44814637, 44814638, 44814639],
            "relationship_id": ["Maps to", "Maps to", "Maps to"],
            "valid_start_date": ["2000-01-01", "2000-01-01", "2000-01-01"],
            "valid_end_date": ["2099-12-31", "2099-12-31", "2099-12-31"],
            "invalid_reason": [None, None, None],
        }
    )
    concept_relationship_df.write_csv(output_dir / "concept_relationship.csv")


def test_e2e_with_notes():
    """Test end-to-end pipeline with NLP feature extraction from notes."""
    with TemporaryDirectory() as temp_dir, TemporaryDirectory() as input_temp_dir:
        root = Path(temp_dir)

        # Create demo OMOP dataset outside the output root so do_overwrite doesn't delete it
        omop_dir = Path(input_temp_dir) / "omop_demo"
        create_demo_omop_with_notes(omop_dir)

        do_overwrite = True
        do_demo = False  # Using custom demo dataset
        do_download = False

        command_parts = [
            "python -m OMOP_MEDS.__main__",
            f"root_output_dir={str(root.resolve())}",
            f"raw_input_dir={str(omop_dir.resolve())}",
            f"do_download={do_download}",
            f"do_overwrite={do_overwrite}",
            f"do_demo={do_demo}",
            # Enable NLP feature extraction
            "+nlp_features.enabled=true",
            "+nlp_features.text_column=note_text",
            "+nlp_features.features=[word_count,char_count,sentence_count,avg_word_length,lexical_diversity]",
            "+nlp_features.prefix=note",
        ]

        full_cmd = " ".join(command_parts)
        command_out = subprocess.run(full_cmd, shell=True, capture_output=True)

        stderr = command_out.stderr.decode()
        stdout = command_out.stdout.decode()

        err_message = (
            f"Command failed with return code {command_out.returncode}.\n"
            f"Command stdout:\n{stdout}\n"
            f"Command stderr:\n{stderr}"
        )
        assert command_out.returncode == 0, err_message

        # Verify data files exist
        data_path = root / "MEDS_cohort" / "data"
        data_files = list(data_path.glob("*.parquet")) + list(
            data_path.glob("**/*.parquet")
        )

        assert len(data_files) > 0, f"No data files found in {data_path}"

        # Verify NLP features were extracted in pre-MEDS output (the final cohort drops them)
        pre_meds_note_path = root / "pre_MEDS" / "note.parquet"
        if pre_meds_note_path.is_dir():
            pre_meds_note_files = list(pre_meds_note_path.rglob("*.parquet"))
            assert pre_meds_note_files, (
                f"No pre-MEDS note parquet files found in {pre_meds_note_path}"
            )
            notes_data = pl.read_parquet(pre_meds_note_files)
        else:
            notes_data = pl.read_parquet(pre_meds_note_path)
        assert notes_data.height > 0, f"No note rows found in {pre_meds_note_path}"

        # Check that NLP feature columns exist
        expected_columns = [
            "note_feature_word_count",
            "note_feature_char_count",
            "note_feature_sentence_count",
            "note_feature_avg_word_length",
            "note_feature_lexical_diversity",
        ]

        for col in expected_columns:
            assert col in notes_data.columns, (
                f"Expected NLP feature column '{col}' not found in output. "
                f"Available columns: {notes_data.columns}"
            )

        # Verify feature values are reasonable
        assert all(v > 0 for v in notes_data["note_feature_word_count"].to_list()), (
            "All notes should have non-zero word count"
        )
        assert all(v > 0 for v in notes_data["note_feature_char_count"].to_list()), (
            "All notes should have non-zero character count"
        )
        assert all(
            v > 0 for v in notes_data["note_feature_lexical_diversity"].to_list()
        ), "All notes should have positive lexical diversity"
        assert all(
            v <= 1 for v in notes_data["note_feature_lexical_diversity"].to_list()
        ), "Lexical diversity should not exceed 1.0"

        # Verify metadata
        metadata_path = root / "MEDS_cohort" / "metadata"

        dataset_metadata = metadata_path / "dataset.json"
        assert dataset_metadata.exists(), "Dataset metadata not found"

        codes_metadata = metadata_path / "codes.parquet"
        assert codes_metadata.exists(), "Codes metadata not found"

        subject_splits = metadata_path / "subject_splits.parquet"
        assert subject_splits.exists(), "Subject splits not found"
