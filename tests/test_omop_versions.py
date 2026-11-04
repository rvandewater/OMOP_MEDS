"""OMOP CDM versions: the per-version pre-MEDS configs and what each version produces.

The configs are checked against the CDM tables and fields of `omop_schema`'s schemas, which its own tests hold
to the official CDM specification, not only against each other: a configured column missing from the input is
skipped without a warning, so a wrong field name silently drops data.
"""

import json
import shutil
from pathlib import Path

import polars as pl
import pytest
from omegaconf import OmegaConf

from OMOP_MEDS import MAIN_CFG
from OMOP_MEDS.__main__ import main as run_omop_meds
from OMOP_MEDS.omop_versions import (
    SUPPORTED_OMOP_VERSIONS,
    get_schema_loader,
    load_pre_meds_tables,
    normalize_omop_version,
    pre_meds_config_path,
)
from OMOP_MEDS.pre_meds import main as pre_meds_main

DEMO_RESOURCES_DIR = Path(__file__).resolve().parent / "demo_resources"


def _cdm_fields(version: str) -> dict[str, dict[str, object]]:
    """{table: {field: Arrow type}} of every CDM table of an OMOP version."""
    schema = get_schema_loader(version)
    return {table: dict(schema.get_schema(table)) for table in schema.get_table_names()}


CDM = {version: _cdm_fields(version) for version in SUPPORTED_OMOP_VERSIONS}
CONFIGS = {
    version: OmegaConf.to_container(load_pre_meds_tables(version))
    for version in SUPPORTED_OMOP_VERSIONS
}

# Configured columns that are no CDM field, per table. They reach the pre-MEDS output when present.
NON_CDM_COLUMNS = {
    # Joined from care_site by the pre-MEDS step.
    "visit_occurrence": {"care_site_name"},
    "note": {
        # Built by the datetime_resolver.
        "preferred_time",
        # Last-edit extension columns that keep notes from leaking into earlier predictions.
        "xtn_note_last_edit_datetime",
        "xtn_note_last_edit_date",
        # In no CDM version; kept for exports that add them.
        "source_concept_id",
        "source_concept_name",
        "encoding_source_value",
        "language_source_value",
    },
    # In no CDM version; kept for exports that add them.
    "condition_era": {"condition_occurrence_duration", "condition_occurrence_length"},
}

CONSECUTIVE_VERSIONS = list(zip(SUPPORTED_OMOP_VERSIONS, SUPPORTED_OMOP_VERSIONS[1:]))


def _configured_columns(block: dict) -> set[str]:
    resolver = block.get("datetime_resolver") or {}
    return {
        *(block.get("reference_cols") or []),
        *(block.get("output_data_cols") or []),
        *resolver.values(),
    }


# --- versions ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("given", "expected"),
    [(5.3, "5.3"), ("5.4", "5.4"), ("v5.4.2", "5.4"), (" 5.5 ", "5.5"), (5.50, "5.5")],
)
def test_normalize_omop_version(given, expected):
    assert normalize_omop_version(given) == expected


@pytest.mark.parametrize("given", ["5.2", "6.0", "5", "five", "", None])
def test_unsupported_omop_versions_are_rejected(given):
    with pytest.raises(
        ValueError,
        match="supported versions: 5.3, 5.4, 5.5. Set OMOP_VERSION to one of them.",
    ):
        normalize_omop_version(given)


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_every_supported_version_has_a_config_and_a_schema(version):
    assert pre_meds_config_path(version).is_file()
    assert get_schema_loader(version).version == version


# --- per-version configs against the CDM -------------------------------------------------------------------


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_configured_tables_and_columns_are_cdm_fields(version):
    unknown = {
        table: sorted(
            _configured_columns(block)
            - set(CDM[version].get(table, {}))
            - NON_CDM_COLUMNS.get(table, set())
        )
        for table, block in CONFIGS[version].items()
    }

    assert set(CONFIGS[version]) <= set(CDM[version])
    assert {table: cols for table, cols in unknown.items() if cols} == {}


@pytest.mark.parametrize(("older", "newer"), CONSECUTIVE_VERSIONS)
def test_tables_the_cdm_left_unchanged_are_configured_alike(older, newer):
    # A fix made in one version's file must be made in the others too.
    unchanged = [
        table
        for table in set(CONFIGS[older]) & set(CONFIGS[newer])
        if CDM[older][table] == CDM[newer][table]
    ]

    assert unchanged, f"no table is unchanged between OMOP {older} and {newer}"
    assert {t: CONFIGS[newer][t] for t in unchanged} == {
        t: CONFIGS[older][t] for t in unchanged
    }


def test_omop_5_5_config_is_the_5_4_config_plus_fields_new_in_5_5():
    def without_5_5_fields(table: str, block: dict) -> dict:
        new = set(CDM["5.5"][table]) - set(CDM["5.4"].get(table, {}))
        return {
            k: [c for c in v if c not in new] if isinstance(v, list) else v
            for k, v in block.items()
        }

    stripped = {t: without_5_5_fields(t, b) for t, b in CONFIGS["5.5"].items()}

    assert stripped == CONFIGS["5.4"]
    assert {
        "value_as_date",
        "value_as_source_concept_id",
        "unit_source_concept_id",
    } <= set(CONFIGS["5.5"]["observation"]["output_data_cols"])
    assert {"visit_occurrence_id", "visit_detail_id"} <= set(
        CONFIGS["5.5"]["specimen"]["output_data_cols"]
    )


@pytest.mark.parametrize(
    ("yaml_text", "match"),
    [
        pytest.param(
            "observation:\n  '5.4':\n    reference_cols: [observation_concept_id]\n",
            r"`observation` must be a table block .* got unknown keys \['5.4'\]",
            id="nested_version_block",
        ),
        pytest.param(
            "subject_id: person_id\n",
            "`subject_id` must be a table block .* got 'person_id'. Shared settings belong in common.yaml",
            id="shared_setting",
        ),
    ],
)
def test_config_blocks_that_are_not_table_blocks_fail_loudly(
    tmp_path: Path, monkeypatch, yaml_text, match
):
    (tmp_path / "omop_5.4.yaml").write_text(yaml_text)
    monkeypatch.setattr("OMOP_MEDS.omop_versions.PRE_MEDS_CFG_DIR", tmp_path)

    with pytest.raises(ValueError, match=match):
        load_pre_meds_tables("5.4")


# --- pre-MEDS and MEDS output per version ------------------------------------------------------------------

# OMOP CDM 5.4 renamed the visit admission and discharge columns (cdm54Changes.html).
VISIT_COLUMNS_RENAMED_IN_5_4 = {
    "admitting_source_concept_id": "admitted_from_concept_id",
    "admitting_source_value": "admitted_from_source_value",
    "discharge_to_concept_id": "discharged_to_concept_id",
    "discharge_to_source_value": "discharged_to_source_value",
}
N_SPECIMENS = 3


def _stage_demo_as(version: str, raw: Path) -> Path:
    """Write the OMOP 5.3 demo resources, plus a few specimens, in the column layout of `version`."""
    assert set(VISIT_COLUMNS_RENAMED_IN_5_4) <= set(CDM["5.3"]["visit_occurrence"])
    assert set(VISIT_COLUMNS_RENAMED_IN_5_4.values()) <= set(
        CDM["5.4"]["visit_occurrence"]
    )

    raw.mkdir(parents=True)
    for fp in DEMO_RESOURCES_DIR.glob("*.csv"):
        shutil.copy2(fp, raw / fp.name)
    pl.read_csv(raw / "concept.csv").select(
        pl.col("concept_id").cast(pl.Int64).alias("concept_id_1"),
        pl.col("concept_id").cast(pl.Int64).alias("concept_id_2"),
    ).with_columns(
        relationship_id=pl.lit("Maps to"),
        valid_start_date=pl.lit("1970-01-01"),
        valid_end_date=pl.lit("2099-12-31"),
        invalid_reason=pl.lit(None, dtype=pl.Utf8),
    ).write_csv(raw / "concept_relationship.csv")

    visits = pl.read_csv(raw / "visit_occurrence.csv", infer_schema=False)
    first_visits = (
        visits.group_by("person_id")
        .agg(
            pl.col(
                "visit_occurrence_id", "visit_start_date", "visit_start_datetime"
            ).min()
        )
        .sort("person_id")
        .head(N_SPECIMENS)
    )
    # Like an OMOP export, with every CDM column of the version, empty where unknown.
    known = {
        "specimen_id": pl.int_range(1, pl.len() + 1).cast(pl.Utf8),
        "person_id": pl.col("person_id"),
        "specimen_concept_id": pl.lit("4001225"),
        "specimen_type_concept_id": pl.lit("32817"),
        "specimen_date": pl.col("visit_start_date"),
        "specimen_datetime": pl.col("visit_start_datetime"),
        "quantity": pl.lit("1"),
        "visit_occurrence_id": pl.col("visit_occurrence_id"),
    }
    first_visits.select(
        known.get(c, pl.lit(None, pl.Utf8)).alias(c) for c in CDM[version]["specimen"]
    ).write_csv(raw / "specimen.csv")

    if version != "5.3":
        visits.rename(VISIT_COLUMNS_RENAMED_IN_5_4).write_csv(
            raw / "visit_occurrence.csv"
        )
    if version == "5.5":
        observations = pl.read_csv(raw / "observation.csv", infer_schema=False)
        every_10th = pl.int_range(pl.len()) % 10 == 0
        observations.with_columns(
            value_as_date=pl.when(every_10th).then(pl.col("observation_date")),
            unit_source_concept_id=pl.when(every_10th).then(pl.lit("8587")),
            value_as_source_concept_id=pl.when(every_10th).then(pl.lit("45877994")),
        ).write_csv(raw / "observation.csv")
    return raw


def _main_cfg(raw: Path, root: Path):
    cfg = OmegaConf.load(MAIN_CFG)
    cfg.raw_input_dir = str(raw)
    cfg.root_output_dir = str(root)
    cfg.pre_MEDS_dir = str(root / "pre_MEDS")
    cfg.MEDS_cohort_dir = str(root / "MEDS_cohort")
    cfg.do_overwrite = True
    return cfg


def _non_null_counts(df: pl.DataFrame, columns: list[str]) -> dict[str, int]:
    return {c: df.height - df[c].null_count() for c in columns}


@pytest.mark.parametrize("version", SUPPORTED_OMOP_VERSIONS)
def test_pre_meds_keeps_each_versions_cdm_columns(tmp_path: Path, monkeypatch, version):
    raw = _stage_demo_as(version, tmp_path / "raw")
    monkeypatch.setenv("OMOP_VERSION", version)

    pre_meds_main(_main_cfg(raw, tmp_path / "out"))

    pre = tmp_path / "out" / "pre_MEDS"
    admission = list(
        VISIT_COLUMNS_RENAMED_IN_5_4.values()
        if version != "5.3"
        else VISIT_COLUMNS_RENAMED_IN_5_4
    )
    persons = pl.read_csv(raw / "person.csv").select("person_id")
    raw_visits = pl.read_csv(raw / "visit_occurrence.csv").join(
        persons, on="person_id", how="semi"
    )
    visits = pl.read_parquet(pre / "visit_occurrence.parquet")
    assert _non_null_counts(visits, admission) == _non_null_counts(
        raw_visits, admission
    )
    assert "preferred_time" in pl.read_parquet(pre / "note.parquet").columns

    specimens = pl.read_parquet(pre / "specimen.parquet")
    observations = pl.read_parquet(pre / "observation.parquet")
    assert specimens.height == N_SPECIMENS
    if version == "5.5":
        new = ["value_as_date", "unit_source_concept_id", "value_as_source_concept_id"]
        raw_observations = pl.read_csv(raw / "observation.csv", infer_schema=False)
        assert _non_null_counts(observations, new) == _non_null_counts(
            raw_observations, new
        )
        assert observations.schema["value_as_date"] == pl.Date
        assert observations.schema["value_as_source_concept_id"] == pl.Int64
        assert specimens["visit_occurrence_id"].null_count() == 0
        assert specimens.schema["visit_occurrence_id"] == pl.Int64
    else:
        assert "value_as_date" not in observations.columns
        assert "visit_occurrence_id" not in specimens.columns


def test_the_same_export_in_each_versions_layout_gives_the_same_meds_events(
    tmp_path: Path, monkeypatch
):
    events = {}
    for version in SUPPORTED_OMOP_VERSIONS:
        raw = _stage_demo_as(version, tmp_path / f"raw_{version}")
        root = tmp_path / f"out_{version}"
        monkeypatch.setenv("OMOP_VERSION", version)

        run_omop_meds.__wrapped__(_main_cfg(raw, root))

        cohort = root / "MEDS_cohort"
        dataset = json.loads((cohort / "metadata" / "dataset.json").read_text())
        assert dataset["dataset_version"].endswith(f":OMOP_{version}")
        df = pl.concat(
            [pl.read_parquet(fp) for fp in sorted((cohort / "data").rglob("*.parquet"))]
        )
        events[version] = df.sort(df.columns, nulls_last=True)

    assert (
        events["5.3"].filter(pl.col("table_name") == "specimen").height == N_SPECIMENS
    )
    assert events["5.4"].equals(events["5.3"])
    assert events["5.5"].equals(events["5.3"])
