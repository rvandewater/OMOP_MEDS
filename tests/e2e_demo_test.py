import json
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema
from omop_run import run_omop

DEMO_RESOURCES_DIR = Path(__file__).resolve().parent / "demo_resources"

# test_e2e downloads from PhysioNet; above the 25-minute download budget, inside the 35-minute CI job timeout.
E2E_TIMEOUT_S = 28 * 60


def _stage_local_demo_omop(raw_input_dir: Path) -> Path:
    """Copy the demo resources into a temp OMOP input dir and add missing tables."""
    raw_input_dir.mkdir(parents=True, exist_ok=True)

    for fp in DEMO_RESOURCES_DIR.glob("*.csv"):
        shutil.copy2(fp, raw_input_dir / fp.name)

    if not (raw_input_dir / "concept_relationship.csv").exists():
        concept_df = pl.read_csv(DEMO_RESOURCES_DIR / "concept.csv")
        concept_relationship_df = concept_df.select(
            pl.col("concept_id").cast(pl.Int64).alias("concept_id_1"),
            pl.col("concept_id").cast(pl.Int64).alias("concept_id_2"),
        ).with_columns(
            relationship_id=pl.lit("Maps to"),
            valid_start_date=pl.lit("1970-01-01"),
            valid_end_date=pl.lit("2099-12-31"),
            invalid_reason=pl.lit(None, dtype=pl.Utf8),
        )
        concept_relationship_df.write_csv(raw_input_dir / "concept_relationship.csv")

    return raw_input_dir


# Expected output over tests/demo_resources, identical under MEDS-Extract 0.6.2 and 0.8.0.
DEMO_N_EVENTS = 33_160
DEMO_N_SUBJECTS = 100
# Observations without a mapped concept name. MEDS-Extract >= 0.7 drops rows whose code has a null component
# unless the event config coalesces it, which these events need to survive.
DEMO_N_UNK_OBSERVATIONS = 1_282
DEMO_EVENT_COLUMNS = {
    "subject_id",
    "time",
    "code",
    "numeric_value",
    "text_value",
    "unit",
    "visit_occurrence_id",
    "link_id",
    "table_name",
    "source_block",
}


@pytest.fixture(scope="module")
def local_demo_cohort(tmp_path_factory) -> Path:
    """Run the ETL once over the checked-in demo resources and return the MEDS cohort directory.

    The plain ETL output: tests/test_run_with_enrichment.py runs the suMEDS stage, offline.
    """
    raw_input_dir = _stage_local_demo_omop(
        tmp_path_factory.mktemp("local_demo_input") / "raw_input"
    )
    cohort = tmp_path_factory.mktemp("local_demo_output") / "MEDS_cohort"
    run_omop(cohort, raw_input_dir, "skip_stages=[sumeds_enrich]")
    return cohort


@pytest.fixture(scope="module")
def local_demo_events(local_demo_cohort: Path) -> pl.DataFrame:
    data_files = sorted((local_demo_cohort / "data").rglob("*.parquet"))
    assert data_files, f"No MEDS data shards under {local_demo_cohort / 'data'}"
    return pl.concat([pl.read_parquet(fp) for fp in data_files])


def test_local_e2e_demo_resources(local_demo_cohort: Path):
    """Run a local end-to-end smoke test against the checked-in demo resources."""
    data_path = local_demo_cohort / "data"
    data_files = list(data_path.glob("**/*.parquet"))
    all_files = [x for x in data_path.glob("**/*") if x.is_file()]

    assert len(data_files) > 0, f"No data files found in {data_path}; found {all_files}"

    metadata_path = local_demo_cohort / "metadata"
    all_files = [x for x in metadata_path.glob("**/*") if x.is_file()]

    dataset_metadata = metadata_path / "dataset.json"
    assert dataset_metadata.exists(), (
        f"Dataset metadata not found in {metadata_path}; found {all_files}"
    )

    codes_metadata = metadata_path / "codes.parquet"
    assert codes_metadata.exists(), (
        f"Codes metadata not found in {metadata_path}; found {all_files}"
    )

    subject_splits = metadata_path / "subject_splits.parquet"
    assert subject_splits.exists(), (
        f"Subject splits not found in {metadata_path}; found {all_files}"
    )


def test_local_e2e_demo_events(local_demo_events: pl.DataFrame):
    events = local_demo_events

    # MEDS-Extract's internal `code_components` linkage column must not leak into the final shards.
    assert set(events.columns) == DEMO_EVENT_COLUMNS
    assert events.height == DEMO_N_EVENTS
    assert events["subject_id"].n_unique() == DEMO_N_SUBJECTS
    assert events["code"].null_count() == 0
    assert (
        events.filter(pl.col("code") == "MEDS_BIRTH")["subject_id"].n_unique()
        == DEMO_N_SUBJECTS
    )
    assert (
        events.filter(pl.col("code") == "OMOP_observation//UNK").height
        == DEMO_N_UNK_OBSERVATIONS
    )


def test_local_e2e_demo_codes_metadata_covers_every_code(
    local_demo_cohort: Path, local_demo_events: pl.DataFrame
):
    codes = pl.read_parquet(local_demo_cohort / "metadata" / "codes.parquet")

    assert set(codes["code"]) == set(local_demo_events["code"])


def test_local_e2e_demo_codes_metadata_is_valid_and_described(local_demo_cohort: Path):
    codes_table = pq.read_table(local_demo_cohort / "metadata" / "codes.parquet")
    CodeMetadataSchema.validate(codes_table)
    codes = pl.from_arrow(codes_table)
    # Observation codes are vocabulary_id//concept_code (mapped and source concept); the demo concept
    # table holds the MIMIC-IV local concepts, which must be described.
    observation = codes.filter(pl.col("table_name") == "observation")
    assert observation["description"].is_not_null().sum() > 0


# Downloads from PhysioNet, which is slow and flaky from CI runners: runs on pushes to main, on a schedule
# and on demand, while pull requests rely on tests/e2e_synthea_test.py.
@pytest.mark.physionet
def test_e2e(tmp_path: Path):
    """`meds-extract-run spec=OMOP dataset_key=demo` downloads the demo and converts it, enrichment included."""
    cohort = tmp_path / "MEDS_cohort"
    # Bounded below the CI job timeout, so a stalled PhysioNet download fails this test with the ETL's
    # logs attached instead of the job being cancelled with no output.
    try:
        run_omop(cohort, None, "dataset_key=demo", timeout=E2E_TIMEOUT_S)
    except subprocess.TimeoutExpired as e:
        pytest.fail(
            f"Command timed out after {E2E_TIMEOUT_S}s.\n"
            f"Command stdout:\n{e.stdout or ''}\n"
            f"Command stderr:\n{e.stderr or ''}"
        )

    data_files = list((cohort / "data").rglob("*.parquet"))
    assert data_files, f"No data files found in {cohort / 'data'}"
    metadata = cohort / "metadata"
    for name in ("dataset.json", "codes.parquet", "subject_splits.parquet"):
        assert (metadata / name).exists(), f"{name} not found in {metadata}"
    # The demo's PhysioNet download, as in the MEDS-Extract download log.
    dataset = json.loads((metadata / "dataset.json").read_text())
    assert dataset["dataset_version"].startswith("0.9:")
    # Enriched, or soft-failed (e.g. GitHub unreachable): either way the run succeeds and says so.
    assert dataset["sumeds_enrichment"]["rules"] == "OMOP"
    events = pl.concat([pl.read_parquet(fp) for fp in data_files])
    assert events["subject_id"].n_unique() == 100
