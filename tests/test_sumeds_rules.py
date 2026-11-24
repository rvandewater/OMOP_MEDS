"""The packaged suMEDS rules load in suMEDS by their registered name, `OMOP`.

suMEDS's `RulesContract` (`sumeds.testing.contracts`) checks that the rules file is packaged, registered under the
spec's name and loads; tests/test_run_with_enrichment.py checks what the rules do to the codes the ETL writes.
"""

from sumeds import load_rules
from sumeds.testing import load_spec, stage_name
from sumeds.testing.contracts import RulesContract


class TestRules(RulesContract):
    rules_name = "OMOP"

    def test_spec_runs_the_sumeds_post_stages_in_order(self):
        """Only `sumeds_enrich`, after the codes metadata. suMEDS's unit stages (`sumeds_link_unit_siblings`,
        `sumeds_normalize_units`) would leave the file as it is: OMOP codes do not end in a unit, and
        codes.parquet has no `unit` column (an event's unit is its `unit_concept_id`)."""
        stages = load_spec("OMOP")["etl"]["post_stages"]
        assert [stage_name(s) for s in stages] == [
            "omop_codes_metadata",
            "sumeds_enrich",
        ]
        assert stages[-1]["sumeds_enrich"]["rules"] == "OMOP"


def test_rules_leave_the_codes_to_the_built_in_layouts():
    """OMOP codes are `vocabulary_id//concept_code`, which suMEDS reads without a rule (see the rules file)."""
    rules = load_rules("OMOP")
    assert (rules.ontology, rules.crosswalk, rules.hierarchy) == ((), (), ())
