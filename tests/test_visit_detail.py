"""visit_detail segments (ICU stays, ward transfers) become `VISIT_DETAIL//...//start` and `//end` events."""

from datetime import time
from pathlib import Path

import polars as pl
import pytest
from omop_run import run_omop
from test_omop_versions import CDM, _stage_demo_as

from OMOP_MEDS.omop_versions import SUPPORTED_OMOP_VERSIONS

ICU = "VISIT_DETAIL//Visit//OMOP4822460"
CONCEPTS = {
    # concept_id: (concept_name, concept_code)
    "32037": ("Intensive Care", "OMOP4822460"),
    "9201": ("Inpatient Visit", "IP"),
}
CARE_SITES = {"1": "Medical ICU", "2": "Cardiology ward"}
# (visit_detail_concept_id, care_site_id, with datetimes, with an end)
SEGMENTS = [
    ("32037", "1", True, True),
    ("9201", "2", True, True),
    # Date-only: timed at the end of the date.
    ("32037", "1", False, True),
    # A care site missing from care_site; and concept 0, which no concept row resolves.
    ("0", "99", True, True),
    # No end date or datetime: a start, but no end event without a time.
    ("9201", "2", True, False),
]


def _write_export(df: pl.DataFrame, cdm_columns, fp: Path) -> None:
    """Like an OMOP export, with every CDM column of the version, empty where unknown."""
    df.select(
        pl.col(c) if c in df.columns else pl.lit(None, pl.Utf8).alias(c)
        for c in cdm_columns
    ).write_csv(fp)


def _stage(version: str, raw: Path) -> pl.DataFrame:
    """The demo in `version`'s layout plus the concepts, care sites and visit_detail rows above."""
    _stage_demo_as(version, raw)
    concept = pl.read_csv(raw / "concept.csv", infer_schema=False)
    new = pl.DataFrame(
        {
            "concept_id": list(CONCEPTS),
            "concept_name": [name for name, _ in CONCEPTS.values()],
            "domain_id": "Visit",
            "vocabulary_id": "Visit",
            "concept_class_id": "Visit",
            "standard_concept": "S",
            "concept_code": [code for _, code in CONCEPTS.values()],
            "valid_start_DATE": "1970-01-01",
            "valid_end_DATE": "2099-12-31",
        }
    )
    pl.concat([concept, new], how="diagonal").write_csv(raw / "concept.csv")
    relationship = pl.read_csv(raw / "concept_relationship.csv", infer_schema=False)
    pl.concat(
        [
            relationship,
            new.select(
                concept_id_1="concept_id",
                concept_id_2="concept_id",
                relationship_id=pl.lit("Maps to"),
                valid_start_date="valid_start_DATE",
                valid_end_date="valid_end_DATE",
            ),
        ],
        how="diagonal",
    ).write_csv(raw / "concept_relationship.csv")
    _write_export(
        pl.DataFrame(
            {
                "care_site_id": list(CARE_SITES),
                "care_site_name": list(CARE_SITES.values()),
            }
        ),
        CDM[version]["care_site"],
        raw / "care_site.csv",
    )

    visits = (
        pl.read_csv(raw / "visit_occurrence.csv", infer_schema=False)
        .sort("visit_occurrence_id")
        .head(len(SEGMENTS))
    )
    concept_id, care_site_id, timed, ended = (list(c) for c in zip(*SEGMENTS))
    segments = visits.select(
        "person_id",
        "visit_occurrence_id",
        pl.int_range(1, pl.len() + 1).cast(pl.Utf8).alias("visit_detail_id"),
        pl.Series("visit_detail_concept_id", concept_id),
        pl.Series("care_site_id", care_site_id),
        pl.lit("32817").alias("visit_detail_type_concept_id"),
        visit_detail_start_date=pl.col("visit_start_date"),
        visit_detail_start_datetime=pl.when(pl.Series(timed)).then(
            pl.col("visit_start_datetime")
        ),
        visit_detail_end_date=pl.when(pl.Series(ended)).then(pl.col("visit_end_date")),
        visit_detail_end_datetime=pl.when(pl.Series(timed) & pl.Series(ended)).then(
            pl.col("visit_end_datetime")
        ),
    )
    _write_export(segments, CDM[version]["visit_detail"], raw / "visit_detail.csv")
    return segments.with_columns(
        pl.col("visit_occurrence_id", "person_id").cast(pl.Int64)
    )


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_visit_detail_segments_become_start_and_end_events(tmp_path: Path, version):
    segments = _stage(version, tmp_path / "raw")
    cohort = tmp_path / "out"
    run_omop(
        cohort,
        tmp_path / "raw",
        "skip_stages=[sumeds_enrich]",
        env={"OMOP_VERSION": version},
    )
    events = pl.concat(
        [pl.read_parquet(fp) for fp in sorted((cohort / "data").rglob("*.parquet"))]
    )

    details = events.filter(pl.col("table_name") == "visit_detail")
    starts = details.filter(pl.col("code").str.ends_with("//start"))
    ends = details.filter(pl.col("code").str.ends_with("//end"))
    assert details["code"].str.starts_with("VISIT_DETAIL//").all()
    assert starts.height == len(SEGMENTS)
    assert ends.height == sum(ended for *_, ended in SEGMENTS)
    assert details["time"].null_count() == 0
    assert details["visit_occurrence_id"].null_count() == 0
    assert set(details["visit_occurrence_id"]) == set(segments["visit_occurrence_id"])

    assert starts.filter(pl.col("code") == f"{ICU}//start").height == 2
    assert ends.filter(pl.col("code") == f"{ICU}//end").height == 2
    assert starts.filter(pl.col("code") == "VISIT_DETAIL//Visit//IP//start").height == 2
    # Concept 0 keeps its event, under the table's fallback vocabulary.
    unmapped = "VISIT_DETAIL//OMOP_visit_detail//UNK"
    assert details.filter(pl.col("code").str.starts_with(unmapped)).height == 2
    # A segment coded IP is no admission: Visit//IP//start counts the IP visits only.
    n_ip_visits = (
        pl.read_csv(tmp_path / "raw" / "visit_occurrence.csv", infer_schema=False)
        .filter(pl.col("visit_concept_id") == "9201")
        .height
    )
    assert n_ip_visits > 0
    assert events.filter(pl.col("code") == "Visit//IP//start").height == n_ip_visits

    # The care site's name on the start; null for a care site missing from care_site.
    assert sorted(starts["text_value"].drop_nulls()) == sorted(
        CARE_SITES[care_site]
        for _, care_site, *_ in SEGMENTS
        if care_site in CARE_SITES
    )
    assert starts["text_value"].null_count() == 1
    assert ends["text_value"].null_count() == ends.height

    # The date-only segment is timed at the end of its dates.
    date_only = segments.filter(pl.col("visit_detail_start_datetime").is_null())
    date_only_events = details.join(
        date_only.select("visit_occurrence_id"), on="visit_occurrence_id"
    )
    assert date_only_events.height == 2
    assert (date_only_events["time"].dt.time() == time(23, 59, 59)).all()

    codes = pl.read_parquet(cohort / "metadata" / "codes.parquet")
    described = dict(
        codes.filter(pl.col("table_name") == "visit_detail")
        .select("code", "description")
        .iter_rows()
    )
    assert described[f"{ICU}//start"] == "Intensive Care"
    assert described[f"{ICU}//end"] == "Intensive Care"
    assert described["VISIT_DETAIL//Visit//IP//start"] == "Inpatient Visit"
