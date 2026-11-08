"""`meds-extract-run spec=OMOP` ends with suMEDS enrichment of codes.parquet (the `sumeds_enrich` stage), offline.

The spec's last stage runs suMEDS with the rules registered as `OMOP` on `metadata/codes.parquet` in place. The
ETL runs over the synthetic demo resources without the stage (`skip_stages=[sumeds_enrich]`) and with it, and the
enriched code metadata must add to the plain one without changing it, and `metadata/dataset.json` records it. A
failed enrichment leaves the plain output, records the failure and does not fail the run unless the stage's
`strict` option is set; `suMEDS-enrich OUTPUT_DIR` then enriches it as the stage would have.

The ontology source is a tiny pack repository in suMEDS's `sumeds-ontologies/0.1` layout, built here and passed
as `SUMEDS_ONTOLOGY_SOURCE` or as the stage's `ontology_source` option, so the test reads nothing over the
network and does not depend on the published packs. The demo resources hold no ICD or LOINC codes, so the packs
cover two of their local MIMIC-IV vocabularies instead, with made-up titles and parents. suMEDS reads
`vocabulary_id//concept_code` codes the same way for every vocabulary.
"""

import csv
import hashlib
import io
import json
import shutil
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from e2e_demo_test import _stage_local_demo_omop
from meds import CodeMetadataSchema, DatasetMetadataSchema
from omop_run import run_omop
from sumeds.cli import enrich_main

# vocabulary -> code -> parent (None for a root). The chart codes are described by the demo's concept table;
# RACE//2000001401 already has the parent mimiciv_per_ethnicity//UNKNOWN.
PACKS = {
    "mimiciv_meas_chart": {
        "PAIN": None,
        "223781": "PAIN",
        "223791": "PAIN",
        "223794": "PAIN",
    },
    "mimiciv_per_ethnicity": {"ANY": None, "UNKNOWN": "ANY", "OTHER": "ANY"},
}


def title(vocabulary: str, code: str) -> str:
    return f"Synthetic {vocabulary} title {code}"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture(scope="module")
def ontology_source(tmp_path_factory) -> Path:
    """A local `sumeds-ontologies/0.1` repository holding the synthetic packs."""
    root = tmp_path_factory.mktemp("ontologies")
    columns = ["code", "description", "parent", "first_year", "last_year"]
    vocabularies = {}
    for vocabulary, rows in PACKS.items():
        release = "test"
        directory = root / vocabulary / release
        directory.mkdir(parents=True)
        by_shard: dict[str, list[list[str]]] = {}
        for code, parent in rows.items():
            by_shard.setdefault(code[:1], []).append(
                [code, title(vocabulary, code), parent or "", "", ""]
            )
        shards = {}
        for key, shard_rows in by_shard.items():
            buffer = io.StringIO()
            csv.writer(buffer, lineterminator="\n").writerows([columns, *shard_rows])
            data = buffer.getvalue().encode()
            (directory / f"{key}.csv").write_bytes(data)
            shards[key] = {
                "file": f"{key}.csv",
                "rows": len(shard_rows),
                "bytes": len(data),
                "sha256": _sha256(data),
            }
        manifest = {
            "schema": "sumeds-ontology-pack/0.1",
            "vocabulary": vocabulary,
            "release": release,
            "columns": columns,
            "fields": {
                "code": "code",
                "description": "description",
                "parent": "parent",
            },
            "shard_key_length": 1,
            "rows": len(rows),
            "bytes": sum(s["bytes"] for s in shards.values()),
            "shards": shards,
        }
        manifest_bytes = json.dumps(manifest).encode()
        (directory / "manifest.json").write_bytes(manifest_bytes)
        vocabularies[vocabulary] = {
            "manifest": f"{vocabulary}/{release}/manifest.json",
            "sha256": _sha256(manifest_bytes),
            "release": release,
            "rows": manifest["rows"],
            "bytes": manifest["bytes"],
        }
    index = {"schema": "sumeds-ontologies/0.1", "vocabularies": vocabularies}
    (root / "index.json").write_text(json.dumps(index))
    return root


def _run(raw_input_dir: Path, out: Path, *args: str, **kwargs):
    """Run the ETL into `out`; return its metadata dir, or the process with `check=False`."""
    result = run_omop(out, raw_input_dir, *args, **kwargs)
    return out / "metadata" if kwargs.get("check", True) else result


def _stage_option(**options) -> str:
    """`overrides=` setting options of the `sumeds_enrich` stage for one run."""
    return (
        "overrides=["
        + ",".join(f"'++stage_cfg.{k}={v}'" for k, v in options.items())
        + "]"
    )


@pytest.fixture(scope="module")
def raw_input_dir(tmp_path_factory) -> Path:
    return _stage_local_demo_omop(tmp_path_factory.mktemp("raw") / "raw_input")


@pytest.fixture(scope="module")
def plain_metadata(tmp_path_factory, raw_input_dir: Path) -> Path:
    return _run(
        raw_input_dir, tmp_path_factory.mktemp("plain"), "skip_stages=[sumeds_enrich]"
    )


@pytest.fixture(scope="module")
def enriched_metadata(
    tmp_path_factory, raw_input_dir: Path, ontology_source: Path
) -> Path:
    return _run(
        raw_input_dir,
        tmp_path_factory.mktemp("enriched"),
        env={"SUMEDS_ONTOLOGY_SOURCE": str(ontology_source)},
    )


def _codes(metadata: Path) -> dict[tuple[str, str | None], dict]:
    table = pq.read_table(metadata / "codes.parquet")
    return {(r["code"], r["table_name"]): r for r in table.to_pylist()}


def _names(value) -> dict[str, str]:
    """A names map as a dict: pyarrow reads `map<string, string>` as a list of (key, value) pairs."""
    return dict(value or ())


def test_codes_parquet_is_replaced_by_valid_meds_metadata(
    plain_metadata: Path, enriched_metadata: Path
):
    CodeMetadataSchema.validate(pq.read_table(enriched_metadata / "codes.parquet"))
    # Enriched in place: no other or leftover temporary file.
    assert sorted(p.name for p in enriched_metadata.iterdir()) == sorted(
        p.name for p in plain_metadata.iterdir()
    )


def test_enrichment_adds_pack_parents_and_their_names(enriched_metadata: Path):
    codes = _codes(enriched_metadata)

    chart = codes["mimiciv_meas_chart//223791", "observation"]
    assert chart["parent_codes"] == ["mimiciv_meas_chart//PAIN"]
    assert _names(chart["parent_names"]) == {
        "mimiciv_meas_chart//PAIN": title("mimiciv_meas_chart", "PAIN")
    }

    # Matched through its existing parent reference, whose ancestors are appended after it.
    race = codes["RACE//2000001401", "person"]
    assert race["parent_codes"] == [
        "mimiciv_per_ethnicity//UNKNOWN",
        "mimiciv_per_ethnicity//ANY",
    ]
    assert _names(race["parent_names"])["mimiciv_per_ethnicity//ANY"] == title(
        "mimiciv_per_ethnicity", "ANY"
    )

    # A code of a vocabulary without a pack is left alone.
    assert not codes["mimiciv_meas_chart//225059", "observation"]["parent_codes"]


def test_enrichment_is_non_destructive(plain_metadata: Path, enriched_metadata: Path):
    plain, enriched = _codes(plain_metadata), _codes(enriched_metadata)
    assert enriched.keys() == plain.keys()

    extended = set()
    for key, before in plain.items():
        after = enriched[key]
        if before["description"] is not None:
            assert after["description"] == before["description"], key
        parents_before = before["parent_codes"] or []
        parents_after = after["parent_codes"] or []
        assert parents_after[: len(parents_before)] == parents_before, key
        if len(parents_after) > len(parents_before):
            extended.add(key[0])
        for column in ("occurrence_count", "concept_id", "vocabulary_id"):
            assert after[column] == before[column], (key, column)
    # Exactly the codes of the packs: the chart codes, and the RACE codes whose parent is in a pack.
    assert extended == {
        "mimiciv_meas_chart//223781",
        "mimiciv_meas_chart//223791",
        "mimiciv_meas_chart//223794",
        "RACE//2000001401",
        "RACE//2000001402",
    }


def test_data_matches_the_plain_etl(plain_metadata: Path, enriched_metadata: Path):
    def events(metadata: Path) -> pl.DataFrame:
        files = sorted((metadata.parent / "data").rglob("*.parquet"))
        return pl.concat([pl.read_parquet(fp) for fp in files]).select(
            "subject_id", "time", "code"
        )

    key = ["subject_id", "time", "code"]
    assert events(enriched_metadata).sort(key).equals(events(plain_metadata).sort(key))


# The ETL writes the rows of codes.parquet in no fixed order, so two builds compare as tables sorted by this key.
KEY = ["code", "table_name"]


def _table(metadata: Path) -> pl.DataFrame:
    return pl.read_parquet(metadata / "codes.parquet").sort(KEY)


def _dataset(metadata: Path) -> dict:
    """`metadata/dataset.json`, which must stay valid MEDS dataset metadata."""
    dataset = json.loads((metadata / "dataset.json").read_text())
    DatasetMetadataSchema.validate(dataset)
    return dataset


def test_dataset_json_records_the_enrichment(
    plain_metadata: Path, enriched_metadata: Path
):
    assert "sumeds_enrichment" not in _dataset(plain_metadata)
    record = _dataset(enriched_metadata)["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == "OMOP"
    assert record["ontology_source"] == "local"
    coverage = record["coverage"]
    assert coverage["codes"] == len(_codes(enriched_metadata))
    assert coverage["described"][0] <= coverage["described"][1]
    assert coverage["with_parent_codes"][0] < coverage["with_parent_codes"][1]


def test_skipped_enrichment_leaves_no_record_and_is_enriched_later(
    tmp_path: Path, plain_metadata: Path, enriched_metadata: Path, ontology_source: Path
):
    assert "sumeds_enrichment" not in _dataset(plain_metadata)
    assert not (plain_metadata.parent / ".logs" / "sumeds_enrich.done").exists()

    # Without a record, `suMEDS-enrich OUTPUT_DIR` needs the rules.
    root = tmp_path / "plain"
    shutil.copytree(plain_metadata.parent, root)
    retry = [str(root), "--rules", "OMOP", "--ontology-source", str(ontology_source)]
    assert enrich_main(retry) == 0
    assert _table(root / "metadata").equals(_table(enriched_metadata))
    assert _dataset(root / "metadata")["sumeds_enrichment"]["enriched"] is True


def test_failed_enrichment_keeps_the_etl_output_and_is_enriched_later(
    tmp_path: Path,
    raw_input_dir: Path,
    ontology_source: Path,
    plain_metadata: Path,
    enriched_metadata: Path,
):
    """No ontology data (as without internet): the run succeeds, unenriched, and says so."""
    missing = tmp_path / "no_ontologies"
    metadata = _run(
        raw_input_dir, tmp_path / "out", _stage_option(ontology_source=missing)
    )
    # The warning goes to the pipeline log, not the console.
    log = (tmp_path / "out" / ".logs" / "pipeline.log").read_text()
    assert "enrichment failed" in log
    assert f"suMEDS-enrich {tmp_path / 'out'}" in log
    assert _table(metadata).equals(_table(plain_metadata))
    record = _dataset(metadata)["sumeds_enrichment"]
    assert record["enriched"] is False
    assert record["rules"] == "OMOP"
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --ontology-source <PACKS_DIR>"
    assert str(missing) not in json.dumps(record)
    # The stage is done: running the same command again does not retry it.
    assert (tmp_path / "out" / ".logs" / "sumeds_enrich.done").exists()

    # `suMEDS-enrich OUTPUT_DIR` reuses the recorded rules and options; the local pack source is a path, so it
    # is given again.
    retry = [str(metadata.parent), "--ontology-source", str(ontology_source)]
    assert enrich_main(retry) == 0
    assert _table(metadata).equals(_table(enriched_metadata))
    record = _dataset(metadata)["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == "OMOP"


def test_strict_enrichment_fails_the_run(
    tmp_path: Path, raw_input_dir: Path, plain_metadata: Path
):
    missing = tmp_path / "no_ontologies"
    result = _run(
        raw_input_dir,
        tmp_path / "out",
        _stage_option(strict="true", ontology_source=missing),
        check=False,
    )
    assert result.returncode != 0
    metadata = tmp_path / "out" / "metadata"
    assert _table(metadata).equals(_table(plain_metadata))
    assert "sumeds_enrichment" not in _dataset(metadata)
    assert not (tmp_path / "out" / ".logs" / "sumeds_enrich.done").exists()
