"""Which OMOP concept each pre-MEDS table resolves its codes from, per OMOP version.

By default the mapped (standard) concept wins and the source concept is the fallback; `prefer_source=True`
reverses that. A table configured with only its `*_source_concept_id` silently ignores the mapped concept: for
OMOP 5.4 that left every Synthea27Nj visit as `OMOP_visit_occurrence//UNK` (its source concept is 0) although
its `visit_concept_id` names the visit type.
"""

import polars as pl
import pytest
from omegaconf import OmegaConf

from OMOP_MEDS.omop_versions import SUPPORTED_OMOP_VERSIONS as OMOP_VERSIONS
from OMOP_MEDS.omop_versions import load_pre_meds_tables
from OMOP_MEDS.pre_meds_utils import SUBJECT_ID, join_concept


def _blocks() -> list[tuple[str, str, dict]]:
    """(table, OMOP version, preprocessor block) for every table in every version's pre-MEDS config."""
    return [
        (table, version, OmegaConf.to_container(block))
        for version in OMOP_VERSIONS
        for table, block in load_pre_meds_tables(version).items()
    ]


@pytest.mark.parametrize(
    ("table", "version", "block"),
    [pytest.param(t, v, b, id=f"{t}-{v}") for t, v, b in _blocks()],
)
def test_source_concept_is_only_a_fallback_for_the_mapped_concept(
    table, version, block
):
    refs = block.get("reference_cols") or []
    for i, col in enumerate(refs):
        if col.endswith("_source_concept_id"):
            mapped = col.removesuffix("_source_concept_id") + "_concept_id"
            assert mapped in refs[:i], (
                f"{table} (OMOP {version}) resolves {col} without {mapped} before it"
            )


def test_omop_versions_resolve_the_same_concept_columns():
    by_table: dict[str, dict] = {}
    for table, version, block in _blocks():
        by_table.setdefault(table, {})[version] = block.get("reference_cols")

    differing = {
        t: refs
        for t, refs in by_table.items()
        if len(set(map(tuple, refs.values()))) > 1
    }
    assert differing == {}


def _block(table: str, version: str) -> dict:
    return OmegaConf.to_container(load_pre_meds_tables(version)[table])


CONCEPTS = pl.LazyFrame(
    {
        "concept_id": [9202, 3012888, 2000000001, 4000, 4001],
        "concept_name": [
            "Outpatient Visit",
            "Diastolic blood pressure",
            "Local code",
            "Progress note",
            "EHR note",
        ],
        "vocabulary_id": ["Visit", "LOINC", "LOCAL", "LOINC", "Note Type"],
        "concept_code": ["OP", "8462-4", "BP-DIA", "11506-3", "NT-EHR"],
    }
)
PERSONS = pl.LazyFrame({SUBJECT_ID: [1]})


def _resolve(table: str, version: str, rows: dict, prefer_source: bool = False):
    block = _block(table, version)
    block.pop("datetime_resolver", None)
    fn = join_concept(table_name=table, **block, prefer_source=prefer_source)
    out = fn(
        pl.LazyFrame({SUBJECT_ID: [1] * len(next(iter(rows.values()))), **rows}),
        CONCEPTS,
        PERSONS,
    )
    return (
        out.select("preferred_vocabulary_name", "preferred_concept_name")
        .collect()
        .rows()
    )


@pytest.mark.parametrize("version", OMOP_VERSIONS)
def test_visit_type_comes_from_visit_concept_id(version):
    # Synthea27Nj: visit_concept_id names the visit type, visit_source_concept_id is 0.
    rows = {"visit_concept_id": [9202], "visit_source_concept_id": [0]}

    assert _resolve("visit_occurrence", version, rows) == [("Visit", "OP")]


@pytest.mark.parametrize("version", OMOP_VERSIONS)
@pytest.mark.parametrize(
    ("prefer_source", "mapped_id", "expected"),
    [
        (False, 3012888, ("LOINC", "8462-4")),
        (True, 3012888, ("LOCAL", "BP-DIA")),
        # A mapped concept missing from the concept table falls back to the source concept.
        (False, 999, ("LOCAL", "BP-DIA")),
    ],
    ids=["mapped_by_default", "prefer_source", "fallback_to_source"],
)
def test_observation_prefers_mapped_concept_unless_prefer_source(
    version, prefer_source, mapped_id, expected
):
    rows = {
        "observation_concept_id": [mapped_id],
        "observation_source_concept_id": [2000000001],
    }

    assert _resolve("observation", version, rows, prefer_source) == [expected]


@pytest.mark.parametrize("version", OMOP_VERSIONS)
@pytest.mark.parametrize(
    ("class_id", "prefer_source", "expected"),
    [
        (4000, False, ("LOINC", "11506-3")),
        # The note type (its provenance, e.g. EHR) only stands in when the class is unknown.
        (0, False, ("Note Type", "NT-EHR")),
        (999, False, ("Note Type", "NT-EHR")),
        # Class and type are not a mapped/source pair, so prefer_source does not reorder them.
        (4000, True, ("LOINC", "11506-3")),
    ],
    ids=["class", "class_0", "class_not_in_vocabulary", "prefer_source"],
)
def test_note_is_coded_by_its_class(version, class_id, prefer_source, expected):
    rows = {"note_class_concept_id": [class_id], "note_type_concept_id": [4001]}

    assert _resolve("note", version, rows, prefer_source) == [expected]
