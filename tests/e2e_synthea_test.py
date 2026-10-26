"""End-to-end run over OHDSI's Synthea27Nj sample, the OMOP 5.4 counterpart to the PhysioNet demo test.

The data is Synthea-generated (synthetic, no real patients): 28 subjects converted to OMOP CDM 5.4 by OHDSI and
published under Apache-2.0 in https://github.com/OHDSI/EunomiaDatasets. It is downloaded at test time (not
vendored), pinned to a commit and verified by SHA-256, so the expected counts below cannot drift with upstream.

Pull requests rely on this test rather than the PhysioNet one: GitHub serves the 3.4 MB archive in about a
second, whereas PhysioNet is slow and unreliable from CI runners. It also covers what the MIMIC-IV demo cannot:
the OMOP 5.4 schema, and DATE columns exported as midnight timestamps ("2005-05-07 00:00:00").
"""

import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest
import requests

pytestmark = pytest.mark.synthea

SYNTHEA_ZIP_URL = (
    "https://raw.githubusercontent.com/OHDSI/EunomiaDatasets/"
    "ae167760e01dbfa1d5e3481c1f7f2aa054aff931/datasets/Synthea27Nj/Synthea27Nj_5.4.zip"
)
SYNTHEA_ZIP_SHA256 = "cdd3971be325e17b0e872bf4560711ab4c346917ecb079e2faf32badb55cb693"
CACHE_DIR_ENV_VAR = "SYNTHEA_OMOP_CACHE_DIR"
E2E_TIMEOUT_S = 10 * 60

N_SUBJECTS = 28
N_DEATHS = 3
# Static demographics carry no timestamp; every other event must have one.
STATIC_CODE_PREFIXES = {"GENDER", "RACE", "ETHNICITY"}
EXPECTED_CODE_PREFIXES = {
    "LOINC",
    "SNOMED",
    "RxNorm",
    "OMOP_visit_occurrence",
    "OMOP_observation_period",
    "MEDS_BIRTH",
    "MEDS_DEATH",
    *STATIC_CODE_PREFIXES,
}


def _sha256(fp: Path) -> str:
    h = hashlib.sha256()
    with open(fp, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _synthea_zip() -> Path:
    """The pinned archive, from the cache dir if present and intact, otherwise downloaded into it."""
    cache_dir = Path(
        os.environ.get(
            CACHE_DIR_ENV_VAR, Path.home() / ".cache" / "OMOP_MEDS" / "test-data"
        )
    ).expanduser()
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_fp = cache_dir / f"Synthea27Nj_5.4-{SYNTHEA_ZIP_SHA256[:12]}.zip"
    if zip_fp.is_file() and _sha256(zip_fp) == SYNTHEA_ZIP_SHA256:
        return zip_fp

    part_fp = zip_fp.with_name(zip_fp.name + ".part")
    response = requests.get(SYNTHEA_ZIP_URL, timeout=(30, 60))
    response.raise_for_status()
    part_fp.write_bytes(response.content)
    if (actual := _sha256(part_fp)) != SYNTHEA_ZIP_SHA256:
        part_fp.unlink()
        raise AssertionError(
            f"{SYNTHEA_ZIP_URL} has SHA-256 {actual}, expected {SYNTHEA_ZIP_SHA256}"
        )
    part_fp.replace(zip_fp)
    return zip_fp


def _extract_tables(zip_fp: Path, raw_input_dir: Path) -> list[str]:
    """Extract the archive's CSVs flat, with the lowercase table names OMOP_MEDS looks up.

    The archive names its files in uppercase (`PERSON.csv`) and also ships macOS `__MACOSX/._*` resource
    forks, which are not tables. Writing by lowercased basename also keeps every file inside `raw_input_dir`.
    """
    raw_input_dir.mkdir(parents=True)
    tables = []
    with zipfile.ZipFile(zip_fp) as zf:
        for member in zf.infolist():
            name = Path(member.filename)
            if (
                member.is_dir()
                or "__MACOSX" in name.parts
                or name.name.startswith("._")
            ):
                continue
            (raw_input_dir / name.name.lower()).write_bytes(zf.read(member))
            tables.append(name.stem.lower())
    return sorted(tables)


def test_e2e_synthea_omop_5_4(tmp_path: Path):
    raw_input_dir = tmp_path / "raw_input"
    tables = _extract_tables(_synthea_zip(), raw_input_dir)
    assert {
        "person",
        "visit_occurrence",
        "condition_era",
        "measurement",
        "death",
        "concept",
    } <= set(tables)

    root = tmp_path / "output"
    command = [
        sys.executable,
        "-m",
        "OMOP_MEDS.__main__",
        f"raw_input_dir={raw_input_dir}",
        f"root_output_dir={root}",
        "do_download=False",
        "do_overwrite=True",
    ]
    env = os.environ | {
        "HYDRA_FULL_ERROR": "1",
        "DATASET_NAME": "SYNTHEA27NJ",
        "RAW_DATASET_VERSION": "5.4",
        "OMOP_VERSION": "5.4",
    }
    try:
        result = subprocess.run(
            command, capture_output=True, env=env, timeout=E2E_TIMEOUT_S
        )
    except subprocess.TimeoutExpired as e:
        pytest.fail(
            f"Command timed out after {E2E_TIMEOUT_S}s.\n"
            f"stdout:\n{(e.stdout or b'').decode()}\nstderr:\n{(e.stderr or b'').decode()}"
        )
    assert result.returncode == 0, (
        f"Command failed with return code {result.returncode}.\n"
        f"stdout:\n{result.stdout.decode()}\nstderr:\n{result.stderr.decode()}"
    )

    metadata = root / "MEDS_cohort" / "metadata"
    for fname in ("dataset.json", "codes.parquet", "subject_splits.parquet"):
        assert (metadata / fname).is_file(), f"{fname} missing from {metadata}"
    dataset = json.loads((metadata / "dataset.json").read_text())
    assert dataset["dataset_name"] == "SYNTHEA27NJ"
    assert dataset["dataset_version"].endswith(":OMOP_5.4"), dataset["dataset_version"]

    data_files = sorted((root / "MEDS_cohort" / "data").rglob("*.parquet"))
    assert data_files, f"No MEDS data shards under {root / 'MEDS_cohort' / 'data'}"
    events = pl.concat([pl.read_parquet(fp) for fp in data_files]).with_columns(
        pl.col("code").str.split("//").list.first().alias("code_prefix")
    )

    assert events["subject_id"].n_unique() == N_SUBJECTS
    prefixes = set(events["code_prefix"].unique().to_list())
    assert EXPECTED_CODE_PREFIXES <= prefixes, (
        f"missing: {EXPECTED_CODE_PREFIXES - prefixes}"
    )
    assert events.filter(pl.col("code_prefix") == "MEDS_DEATH").height == N_DEATHS
    assert (
        events.filter(pl.col("code_prefix") == "MEDS_BIRTH")["subject_id"].n_unique()
        == N_SUBJECTS
    )
    untimed = set(
        events.filter(pl.col("time").is_null())["code_prefix"].unique().to_list()
    )
    assert untimed <= STATIC_CODE_PREFIXES, (
        f"events without a time: {untimed - STATIC_CODE_PREFIXES}"
    )
