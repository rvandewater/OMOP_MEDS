import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path
from tempfile import TemporaryDirectory
import os

import polars as pl
import pytest
from omegaconf import OmegaConf

from OMOP_MEDS.__main__ import main as run_omop_meds
from OMOP_MEDS import MAIN_CFG

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


def _path_without(directory: str) -> str:
    target = Path(directory).resolve()
    return os.pathsep.join(
        p
        for p in os.environ.get("PATH", "").split(os.pathsep)
        if p and Path(p).resolve() != target
    )


@pytest.fixture(scope="module")
def local_demo_cohort(tmp_path_factory) -> Path:
    """Run the ETL once over the checked-in demo resources and return the MEDS cohort directory.

    This interpreter's scripts directory is removed from `PATH` first, so the run passes only if the ETL
    launches MEDS-Transforms from its own environment rather than whatever `PATH` would find.
    """
    # Separate from the output root, which do_overwrite=True deletes.
    raw_input_dir = _stage_local_demo_omop(
        tmp_path_factory.mktemp("local_demo_input") / "raw_input"
    )
    root = tmp_path_factory.mktemp("local_demo_output")

    cfg = OmegaConf.load(MAIN_CFG)
    cfg.root_output_dir = str(root.resolve())
    cfg.raw_input_dir = str(raw_input_dir.resolve())
    cfg.pre_MEDS_dir = str((root / "pre_MEDS").resolve())
    cfg.MEDS_cohort_dir = str((root / "MEDS_cohort").resolve())
    cfg.do_download = False
    cfg.do_demo = False
    cfg.do_overwrite = True
    cfg.prefer_source = False
    cfg.join_on_visit = False

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PATH", _path_without(sysconfig.get_path("scripts")))
        run_omop_meds.__wrapped__(cfg)

    return root / "MEDS_cohort"


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


# Downloads from PhysioNet, which is slow and flaky from CI runners: runs on pushes to main, on a schedule
# and on demand, while pull requests rely on tests/e2e_synthea_test.py.
@pytest.mark.physionet
def test_e2e():
    with TemporaryDirectory() as temp_dir:
        os.environ["HYDRA_FULL_ERROR"] = "1"
        root = Path(temp_dir)

        do_overwrite = True
        do_demo = True
        do_download = True

        # sys.executable (not `python` on PATH, without a shell) so the run uses this environment's package
        # and the timeout below kills the ETL process itself rather than an intermediate shell.
        command = [
            sys.executable,
            "-m",
            "OMOP_MEDS.__main__",
            f"root_output_dir={str(root.resolve())}",
            f"do_download={do_download}",
            f"do_overwrite={do_overwrite}",
            f"do_demo={do_demo}",
        ]

        # Bounded below the CI job timeout, so a stalled PhysioNet download fails this test with the ETL's
        # logs attached instead of the job being cancelled with no output.
        try:
            command_out = subprocess.run(
                command, capture_output=True, timeout=E2E_TIMEOUT_S
            )
        except subprocess.TimeoutExpired as e:
            pytest.fail(
                f"Command timed out after {E2E_TIMEOUT_S}s.\n"
                f"Command stdout:\n{(e.stdout or b'').decode()}\n"
                f"Command stderr:\n{(e.stderr or b'').decode()}"
            )

        stderr = command_out.stderr.decode()
        stdout = command_out.stdout.decode()

        err_message = (
            f"Command failed with return code {command_out.returncode}.\n"
            f"Command stdout:\n{stdout}\n"
            f"Command stderr:\n{stderr}"
        )
        assert command_out.returncode == 0, err_message

        data_path = root / "MEDS_cohort" / "data"
        data_files = list(data_path.glob("*.parquet")) + list(
            data_path.glob("**/*.parquet")
        )

        all_files = [x for x in data_path.glob("**/*") if x.is_file()]

        assert len(data_files) > 0, (
            f"No data files found in {data_path}; found {all_files}"
        )

        metadata_path = root / "MEDS_cohort" / "metadata"
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
