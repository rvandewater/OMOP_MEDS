"""Which OMOP concept each pre-MEDS table resolves its codes from, per OMOP version.

By default the mapped (standard) concept wins and the source concept is the fallback; `prefer_source=True`
reverses that. A table configured with only its `*_source_concept_id` silently ignores the mapped concept: for
OMOP 5.4 that left every Synthea27Nj visit as `OMOP_visit_occurrence//UNK` (its source concept is 0) although
its `visit_concept_id` names the visit type.
"""

import polars as pl
import pytest
import yaml

from OMOP_MEDS import PRE_MEDS_CFG
from OMOP_MEDS.pre_meds_utils import SUBJECT_ID, join_concept

OMOP_VERSIONS = (5.3, 5.4)


def _blocks() -> list[tuple[str, object, dict]]:
    """(table, OMOP version or None, preprocessor block) for every table in the active pre-MEDS config."""
    out = []
    for table, block in yaml.safe_load(PRE_MEDS_CFG.read_text()).items():
        if not isinstance(block, dict):
            continue
        versioned = [v for v in OMOP_VERSIONS if v in block]
        if versioned:
            out.extend((table, v, block[v]) for v in versioned)
        elif "reference_cols" in block:
            out.append((table, None, block))
    return out


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
        if version is not None:
            by_table.setdefault(table, {})[version] = block.get("reference_cols")

    differing = {
        t: refs
        for t, refs in by_table.items()
        if len(set(map(tuple, refs.values()))) > 1
    }
    assert differing == {}


def _block(table: str, version: float) -> dict:
    return yaml.safe_load(PRE_MEDS_CFG.read_text())[table][version]


CONCEPTS = pl.LazyFrame(
    {
        "concept_id": [9202, 3012888, 2000000001],
        "concept_name": ["Outpatient Visit", "Diastolic blood pressure", "Local code"],
        "vocabulary_id": ["Visit", "LOINC", "LOCAL"],
        "concept_code": ["OP", "8462-4", "BP-DIA"],
    }
)
PERSONS = pl.LazyFrame({SUBJECT_ID: [1]})


def _resolve(table: str, version: float, rows: dict, prefer_source: bool = False):
    fn = join_concept(
        table_name=table, **_block(table, version), prefer_source=prefer_source
    )
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
