# OMOP CDM field lists

`cdm_v<version>_fields.csv` lists every table field of OMOP CDM 5.3, 5.4 and 5.5: table, field, datatype and
whether it is required, lowercased. `tests/test_omop_versions.py` checks the pre-MEDS configs and the OMOP 5.5
schema against them.

They are trimmed from OHDSI's `inst/csv/OMOP_CDMv<version>_Field_Level.csv` at the
[`v5.5.0` tag](https://github.com/OHDSI/CommonDataModel/tree/v5.5.0/inst/csv) (Apache-2.0), keeping four columns
and dropping a blank `NA` row from the 5.3 file.
