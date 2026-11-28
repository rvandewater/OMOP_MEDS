"""Codes built from an OMOP concept end in its `concept_code`, never in its `concept_id`.

A `concept_id` can equal the `concept_code` of another concept in the same vocabulary, so a code ending in one
collides with a real code (#48). All vocabularies, codes and names below are synthetic.
"""

import polars as pl
import pytest
from dftly import Parser
from omegaconf import OmegaConf

from OMOP_MEDS import EVENT_CFG
from OMOP_MEDS.omop_versions import SUPPORTED_OMOP_VERSIONS, load_pre_meds_tables
from OMOP_MEDS.pre_meds_utils import SUBJECT_ID, join_concept

# Concept 10's concept_code is "20", and concept 20 exists in the same vocabulary.
CONCEPT = pl.LazyFrame(
    {
        "concept_id": [10, 20],
        "concept_name": ["Ten", "Twenty"],
        "vocabulary_id": ["VOC", "VOC"],
        "concept_code": ["20", "D20"],
    }
)
# Concept 999 is not in the concept table; 0 means "no concept".
CONCEPT_IDS = [10, 20, 999, 0]
TABLES = ["drug_exposure", "drug_era", "condition_era", "specimen", "device_exposure"]
BOUNDARY = r"//(start|end)$"


def _codes(table: str, version: str) -> pl.DataFrame:
    """(event, code without //start or //end, concept_id) for each concept in `CONCEPT_IDS`."""
    block = OmegaConf.to_container(load_pre_meds_tables(version)[table])
    block.pop("datetime_resolver", None)
    rows = {SUBJECT_ID: [1] * len(CONCEPT_IDS)}
    rows |= {col: CONCEPT_IDS for col in block["reference_cols"]}
    pre_meds = join_concept(table, **block)(
        pl.LazyFrame(rows).with_columns(table_name=pl.lit(table)),
        CONCEPT,
        pl.LazyFrame({SUBJECT_ID: [1]}),
    )
    events = OmegaConf.load(EVENT_CFG)[table]
    return pl.concat(
        pre_meds.select(
            event=pl.lit(name),
            code=Parser()(str(event["code"])).polars_expr.str.replace(BOUNDARY, ""),
            concept_id=pl.col("preferred_concept_id").fill_null(0),
        ).collect()
        for name, event in events.items()
    )


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
@pytest.mark.parametrize("table", TABLES)
def test_codes_end_in_the_concept_code(table, version):
    codes = _codes(table, version)
    by_concept = dict(
        codes.group_by("concept_id").agg(pl.col("code").unique().sort()).iter_rows()
    )
    first_col = load_pre_meds_tables(version)[table]["reference_cols"][0]

    assert by_concept[10] == ["VOC//20"]
    assert by_concept[20] == ["VOC//D20"]
    # A concept missing from the concept table names its column and id, under a prefix no vocabulary has.
    [missing] = by_concept[999]
    assert missing.startswith(f"OMOP_{table}//{first_col}:999")
    assert by_concept[0] == [f"OMOP_{table}//UNK"]
    # No two concepts render the same code.
    assert (
        codes.group_by("code").agg(pl.col("concept_id").n_unique())["concept_id"].max()
        == 1
    )


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_device_start_and_end_codes_match(version):
    codes = _codes("device_exposure", version)
    start, end = (
        codes.filter(pl.col("event") == event).drop("event").sort("concept_id").rows()
        for event in ("device_exposure", "device_exposure_end")
    )

    assert len(start) == len(CONCEPT_IDS)
    assert start == end
