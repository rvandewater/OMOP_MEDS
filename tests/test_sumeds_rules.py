"""The packaged suMEDS rules load in suMEDS by their registered name, `OMOP`.

The runner's enrichment step and `suMEDS-enrich --rules OMOP` find them through this name. suMEDS validates the
file itself (`sumeds.load_rules` rejects malformed rules); tests/test_run_with_enrichment.py checks what they do
to the codes the ETL writes.
"""

from importlib.resources import files
from pathlib import Path

from sumeds import load_rules, registered_rules

RULES_FP = Path(__file__).parents[1] / "src" / "OMOP_MEDS" / "sumeds_rules.yaml"


def test_rules_file_is_packaged():
    assert files("OMOP_MEDS").joinpath("sumeds_rules.yaml").is_file()


def test_rules_are_registered_as_omop():
    assert "OMOP" in registered_rules()
    assert registered_rules()["OMOP"] == "OMOP_MEDS:sumeds_rules.yaml"
    assert load_rules("OMOP") == load_rules(RULES_FP)


def test_rules_leave_the_codes_to_the_built_in_layouts():
    """OMOP codes are `vocabulary_id//concept_code`, which suMEDS reads without a rule (see the rules file)."""
    rules = load_rules("OMOP")
    assert (rules.ontology, rules.crosswalk, rules.hierarchy) == ((), (), ())
