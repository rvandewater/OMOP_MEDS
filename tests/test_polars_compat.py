"""Regression tests for code paths that differ between Polars 1.x and 2.x.

MEDS-Transforms 0.7 allows Polars 2, so `pip install` (as in CI) picks it up while `uv.lock` still pins 1.x; the
package has to work, with identical output, on both.
"""

from pathlib import Path

import polars as pl

import OMOP_MEDS
from OMOP_MEDS.pre_meds_utils import extract_codes_metadata

SRC_DIR = Path(OMOP_MEDS.__file__).parent


def test_parent_codes_are_single_element_lists_or_null():
    # Polars 2 no longer casts String to List(String); the replacement must keep 1.x's output exactly.
    # IDs are Int64, as the OMOP schema loader types them.
    concept = pl.LazyFrame(
        {
            "concept_id": [1, 2, 3],
            "vocabulary_id": ["ICD10", "SNOMED", "LOINC"],
            "concept_code": ["A01", "C2", "L3"],
            "concept_name": ["source", "standard", "unmapped"],
        }
    )
    concept_relationship = pl.LazyFrame(
        {
            "concept_id_1": [1, 1, 3],
            "concept_id_2": [2, 3, 2],
            "relationship_id": ["Maps to", "Is a", "Mapped from"],
        }
    )

    out = (
        extract_codes_metadata(concept, concept_relationship)
        .collect()
        .sort("concept_id")
    )

    assert out.schema["parent_codes"] == pl.List(pl.String)
    assert out.select("code", "parent_codes").rows() == [
        ("ICD10//1", ["SNOMED//C2"]),
        ("SNOMED//2", None),
        ("LOINC//3", None),
    ]


def test_no_imports_from_modules_removed_in_polars_2():
    # Only a DeprecationWarning on Polars 1.x, so this would otherwise pass locally and fail on Polars 2.
    offenders = [
        str(fp.relative_to(SRC_DIR))
        for fp in SRC_DIR.rglob("*.py")
        if "polars.type_aliases" in fp.read_text()
    ]
    assert offenders == []
