"""`meds-extract-run spec=OMOP` ends with suMEDS enrichment of codes.parquet (the `sumeds_enrich` stage), offline.

suMEDS's `EnrichmentContract` (`sumeds.testing.contracts`) holds the tests every ETL that enriches with suMEDS
shares: the ETL runs over the synthetic demo resources without the stage (`skip_stages=[sumeds_enrich]`) and with
it; the enriched code metadata adds to the plain one without changing it, and `metadata/dataset.json` records it;
a failed enrichment keeps the plain output and can be retried with `suMEDS-enrich`; `strict` fails the run; and
`offline` reads only the cache. The tests below add what is particular to OMOP.

The ontology source is a tiny pack repository in suMEDS's `sumeds-ontologies/0.1` layout, built by the contract
from `packs`, so the test reads nothing over the network and does not depend on the published packs. The demo
resources hold no ICD or LOINC codes, so the packs cover two of their local MIMIC-IV vocabularies instead, with
made-up titles and parents. suMEDS reads `vocabulary_id//concept_code` codes the same way for every vocabulary.
"""

import shutil
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from e2e_demo_test import _stage_local_demo_omop
from sumeds.cli import enrich_main
from sumeds.testing.contracts import EnrichmentContract


class TestEnrichment(EnrichmentContract):
    rules_name = "OMOP"
    # vocabulary -> code -> parent (None for a root). The chart codes are described by the demo's concept table;
    # RACE//2000001401 already has the parent mimiciv_per_ethnicity//UNKNOWN.
    packs = {
        "mimiciv_meas_chart": {
            "PAIN": None,
            "223781": "PAIN",
            "223791": "PAIN",
            "223794": "PAIN",
        },
        "mimiciv_per_ethnicity": {"ANY": None, "UNKNOWN": "ANY", "OTHER": "ANY"},
    }
    expected_parents = {
        "mimiciv_meas_chart//223791": ["mimiciv_meas_chart//PAIN"],
        # Matched through its existing parent reference, whose ancestors are appended after it.
        "RACE//2000001401": [
            "mimiciv_per_ethnicity//UNKNOWN",
            "mimiciv_per_ethnicity//ANY",
        ],
    }
    # A code of a vocabulary without a pack.
    unenriched_codes = ["mimiciv_meas_chart//225059"]

    @pytest.fixture(scope="class")
    def raw_dir(self, tmp_path_factory) -> Path:
        return _stage_local_demo_omop(tmp_path_factory.mktemp("raw") / "raw_input")

    def test_enrichment_extends_exactly_the_pack_codes_and_keeps_the_omop_columns(
        self, plain_output: Path, enriched_output: Path
    ):
        """Keyed by (code, table_name): the same code can come from several OMOP tables."""

        def rows(output: Path) -> dict[tuple[str, str | None], dict]:
            table = pq.read_table(output / "metadata" / "codes.parquet")
            return {(r["code"], r["table_name"]): r for r in table.to_pylist()}

        plain, enriched = rows(plain_output), rows(enriched_output)
        assert enriched.keys() == plain.keys()
        extended = set()
        for key, before in plain.items():
            after = enriched[key]
            if len(after["parent_codes"] or []) > len(before["parent_codes"] or []):
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

    def test_skipped_enrichment_is_enriched_later_with_the_rules(
        self,
        tmp_path: Path,
        plain_output: Path,
        enriched_output: Path,
        ontology_source: Path,
    ):
        """Without a record in dataset.json, `suMEDS-enrich OUTPUT_DIR` needs the rules."""
        root = tmp_path / "plain"
        shutil.copytree(plain_output, root)
        retry = [
            str(root),
            "--rules",
            "OMOP",
            "--ontology-source",
            str(ontology_source),
        ]
        assert enrich_main(retry) == 0

        key = ["code", "table_name"]
        enriched = pl.read_parquet(root / "metadata" / "codes.parquet").sort(key)
        assert enriched.equals(
            pl.read_parquet(enriched_output / "metadata" / "codes.parquet").sort(key)
        )
