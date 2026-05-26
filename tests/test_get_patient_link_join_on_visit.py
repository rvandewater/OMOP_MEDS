import polars as pl
from omop_schema.utils import get_schema_loader

from OMOP_MEDS.pre_meds_utils import get_patient_link


def test_get_patient_link_includes_patients_without_visits_when_join_on_visit_false():
    schema_loader = get_schema_loader(5.3)

    # person_id=3 has no visit and should still be included when join_on_visit=False.
    person_df = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "year_of_birth": [1980, 1990, 2000],
            "month_of_birth": [1, 2, 3],
            "day_of_birth": [1, 2, 3],
            "birth_datetime": [None, None, None],
        }
    ).lazy()

    visit_df = pl.DataFrame({"person_id": [1, 2]}).lazy()

    death_df = pl.DataFrame(
        {
            "person_id": [1],
            "death_datetime": ["2020-01-01 00:00:00"],
        }
    ).lazy()

    out_no_visit_join = get_patient_link(
        person_df=person_df,
        death_df=death_df,
        visit_df=visit_df,
        schema_loader=schema_loader,
        join_on_visit=False,
    ).collect()

    assert set(out_no_visit_join["person_id"].to_list()) == {1, 2, 3}

    death_by_person = {
        row["person_id"]: row["date_of_death"]
        for row in out_no_visit_join.select(["person_id", "date_of_death"]).to_dicts()
    }
    assert death_by_person[1] is not None
    assert death_by_person[2] is None
    assert death_by_person[3] is None


def test_get_patient_link_filters_patients_without_visits_when_join_on_visit_true():
    schema_loader = get_schema_loader(5.3)

    person_df = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "year_of_birth": [1980, 1990, 2000],
            "month_of_birth": [1, 2, 3],
            "day_of_birth": [1, 2, 3],
            "birth_datetime": [None, None, None],
        }
    ).lazy()

    visit_df = pl.DataFrame({"person_id": [1, 2]}).lazy()

    death_df = pl.DataFrame(
        {
            "person_id": [1],
            "death_datetime": ["2020-01-01 00:00:00"],
        }
    ).lazy()

    out_with_visit_join = get_patient_link(
        person_df=person_df,
        death_df=death_df,
        visit_df=visit_df,
        schema_loader=schema_loader,
        join_on_visit=True,
    ).collect()

    assert set(out_with_visit_join["person_id"].to_list()) == {1, 2}

    death_by_person = {
        row["person_id"]: row["date_of_death"]
        for row in out_with_visit_join.select(["person_id", "date_of_death"]).to_dicts()
    }
    assert death_by_person[1] is not None
    assert death_by_person[2] is None
