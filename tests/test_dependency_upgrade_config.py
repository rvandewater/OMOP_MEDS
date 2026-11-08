"""Guards for the MEDS-Extract (>= 0.8.1) / MEDS-Transforms (>= 0.7) spec and stages this package ships.

MEDS-Extract 0.7 reworked the pipeline and MESSY config formats; mistakes there either fail late inside a
stage subprocess or, worse, silently drop events. These tests check the shipped spec against the installed
MEDS-Extract itself, so a future upgrade that changes its canonical pipeline or parsing rules fails here first.
"""

import re
from importlib.metadata import entry_points
from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest
import yaml
from MEDS_extract.config import EventConfig, MessyConfig

from OMOP_MEDS import EVENT_CFG

RESERVED_KEYS = {"etl", "sources", "_defaults"}


def _stage_names(stages: list) -> list[str]:
    return [next(iter(s)) if isinstance(s, dict) else s for s in stages]


def _pipeline_cfg(**kwargs) -> dict:
    """The pipeline config `meds-extract-run spec=OMOP` synthesizes."""
    return MessyConfig.load("OMOP").pipeline_config(
        input_dir=Path("/raw"), output_dir=Path("/out"), **kwargs
    )


def _event_cfg() -> dict:
    return yaml.safe_load(EVENT_CFG.read_text())


def _code_events() -> list[tuple[str, str, str]]:
    """(table, event, code expression) for every event in the shipped event config."""
    return [
        (table, event, spec["code"])
        for table, events in _event_cfg().items()
        if table not in RESERVED_KEYS
        for event, spec in events.items()
    ]


# --- the registered spec and its pipeline -------------------------------------------------------------------


def test_spec_is_registered_as_omop():
    (ep,) = [
        ep for ep in entry_points(group="MEDS_extract.pipelines") if ep.name == "OMOP"
    ]
    messy = MessyConfig.load("OMOP")

    assert ep.value == "OMOP_MEDS.configs:event_configs.yaml"
    assert Path(messy.source_fp) == Path(str(EVENT_CFG))
    assert messy.spec_ref == "pkg://OMOP_MEDS.configs.event_configs.yaml"
    assert messy.dataset_name == "OMOP"


def test_pipeline_is_the_canonical_one_between_the_omop_stages():
    # The installed MEDS-Extract ships its canonical pipeline; the spec only adds extension stages around it.
    canonical = yaml.safe_load(
        files("MEDS_extract.configs").joinpath("_extract.yaml").read_text()
    )["stages"]

    assert _stage_names(_pipeline_cfg()["stages"]) == [
        "omop_pre_meds",
        *_stage_names(canonical),
        "omop_codes_metadata",
        "sumeds_enrich",
    ]


def test_extension_stages_are_installed_and_of_the_right_kind():
    registered = {ep.name for ep in entry_points(group="MEDS_transforms.stages")}

    assert {"omop_pre_meds", "omop_codes_metadata", "sumeds_enrich"} <= registered
    MessyConfig.load("OMOP").etl.check_extension_stages()


def test_enrichment_uses_the_omop_rules_and_can_be_skipped():
    (enrich,) = [
        s
        for s in _pipeline_cfg()["stages"]
        if isinstance(s, dict) and "sumeds_enrich" in s
    ]
    assert enrich["sumeds_enrich"]["rules"] == "OMOP"
    # Athena is not passed on (see the README): suMEDS matches code strings, and OMOP codes may end in a concept_id.
    assert "athena_csv" not in enrich["sumeds_enrich"]

    skipped = _stage_names(_pipeline_cfg(skip_stages=["sumeds_enrich"])["stages"])
    assert "sumeds_enrich" not in skipped
    assert skipped[-1] == "omop_codes_metadata"


def test_pipeline_options_and_version_stamps():
    cfg = _pipeline_cfg()
    (split,) = [
        s
        for s in cfg["stages"]
        if isinstance(s, dict) and "split_and_shard_subjects" in s
    ]

    assert split["split_and_shard_subjects"]["n_subjects_per_shard"] == 10_000
    assert cfg["etl_metadata"]["dataset_name"] == "OMOP"
    # Any OMOP dataset: its version is unknown unless given (dataset_version=); the demo's is 0.9.
    assert cfg["etl_metadata"]["dataset_version"].startswith("unknown:")
    assert _pipeline_cfg(key="demo")["etl_metadata"]["dataset_version"].startswith(
        "0.9:"
    )
    assert (
        _pipeline_cfg(dataset_version="v1")["etl_metadata"]["dataset_version"] == "v1"
    )


# --- event config (event_configs.yaml) --------------------------------------------------------------------


def test_event_config_is_valid_messy():
    cfg = MessyConfig.load(EVENT_CFG)

    expected_tables = set(_event_cfg()) - RESERVED_KEYS
    assert set(cfg.table_prefixes) == expected_tables
    for table in cfg.event_tables:
        assert table.subject_id_node.referenced_columns == {"person_id"}, (
            table.input_prefix
        )


def test_event_config_has_no_legacy_syntax():
    raw = _event_cfg()
    text = EVENT_CFG.read_text()

    assert "subject_id_col" not in raw, "replaced by _defaults.subject_id in 0.7"
    assert raw["_defaults"] == {"subject_id": "$person_id"}
    assert "col(" not in text
    assert "time_format" not in text


@pytest.mark.parametrize(
    "dtype", [pl.String, pl.Int64], ids=["string_cols", "int_cols"]
)
@pytest.mark.parametrize(
    ("table", "event", "code"),
    [
        pytest.param(table, event, code, id=f"{table}-{event}")
        for table, event, code in _code_events()
        if "$" in code
    ],
)
def test_null_code_components_render_as_unk(table, event, code, dtype):
    """Every interpolated component must fall back to "UNK", as it did before MEDS-Extract 0.7.

    Since 0.7 a null component nulls the whole code and MEDS-Extract drops the row (e.g. the demo's 1,282
    observations without a resolvable concept), so a new code added without `?? 'UNK'` fails here.
    """
    cols = sorted(set(re.findall(r"\$(\w+)", code)))
    present = 7 if dtype == pl.Int64 else "X"
    raw = pl.DataFrame(
        {
            "subject_id": [1, 2],
            **{c: pl.Series([present, None], dtype=dtype) for c in cols},
        }
    )

    out = (
        EventConfig.parse(event, {"code": code, "time": None})
        .extract(raw.lazy(), f"{table}/{event}")
        .collect()
        .sort("subject_id")
    )

    assert code.startswith('f"') and code.endswith('"'), code
    template = code[2:-1]
    assert out["code"].to_list() == [
        re.sub(r"\{[^}]*\}", str(present), template),
        re.sub(r"\{[^}]*\}", "UNK", template),
    ]
