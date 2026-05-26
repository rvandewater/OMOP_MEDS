import polars as pl

from OMOP_MEDS.pre_meds_utils import join_concept


def test_join_concept_does_not_accumulate_output_columns_across_calls():
    func = join_concept(
        table_name="observation",
        reference_cols=["observation_concept_id", "observation_source_concept_id"],
        output_data_cols=["observation_concept_id", "observation_source_concept_id"],
        concept_cols=[
            "vocabulary_id",
            "concept_code",
            "vocabulary_id_source_concept_id",
            "concept_code_source_concept_id",
        ],
        prefer_source=False,
    )

    df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "observation_concept_id": [100, 101],
            "observation_source_concept_id": [200, 201],
        }
    ).lazy()

    concept_df = pl.DataFrame(
        {
            "concept_id": [100, 101, 200, 201],
            "vocabulary_id": ["A", "A", "B", "B"],
            "concept_code": ["C100", "C101", "C200", "C201"],
        }
    ).lazy()

    person_df = pl.DataFrame({"person_id": [1, 2]}).lazy()

    first = func(df, concept_df, person_df)
    second = func(df, concept_df, person_df)

    # Both calls should produce a valid schema without duplicate projection names.
    first_names = first.collect_schema().names()
    second_names = second.collect_schema().names()

    assert len(first_names) == len(set(first_names))
    assert len(second_names) == len(set(second_names))
    assert first_names == second_names
