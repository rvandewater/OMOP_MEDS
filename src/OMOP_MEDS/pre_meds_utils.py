from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger
from omop_schema.convert import convert_to_schema_polars
from omop_schema.schema.base import OMOPSchemaBase
from omop_schema.utils import pyarrow_to_polars_schema

from . import dataset_info, premeds_cfg

DATASET_NAME = dataset_info.dataset_name
ADMISSION_ID = premeds_cfg.admission_id
SUBJECT_ID = premeds_cfg.subject_id
OMOP_TIME_FORMATS: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d")


def get_table_path(input_dir: Path, table_name: str) -> Path | None:
    table_path = input_dir / table_name
    if table_path.exists():
        return table_path
    table_path_with_ext = list(input_dir.glob(f"{table_name}.*"))
    if table_path_with_ext:
        return table_path_with_ext[0]
    return None


def parse_time(time: pl.Expr, time_formats: Iterable[str]) -> pl.Expr:
    return pl.coalesce(
        [time.str.to_datetime(time_format, strict=False, time_unit="us") for time_format in time_formats]
    )


def cast_to_datetime(schema: Any, column: str, move_to_end_of_day: bool = False):
    if schema[column] == pl.Utf8():
        if not move_to_end_of_day:
            return parse_time(pl.col(column), OMOP_TIME_FORMATS)
        else:
            # Try to cast time to a datetime but if only the date is available, then use
            # that date with a timestamp of 23:59:59
            time = pl.col(column)
            time = pl.coalesce(
                time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="us"),
                time.str.to_datetime("%Y-%m-%d", strict=False, time_unit="us")
                .dt.offset_by("1d")
                .dt.offset_by("-1s"),
            )
            return time
    elif schema[column] == pl.Date():
        time = pl.col(column).cast(pl.Datetime(time_unit="us"))
        if move_to_end_of_day:
            time = time.dt.offset_by("1d").dt.offset_by("-1s")
        return time
    elif isinstance(schema[column], pl.Datetime):
        return pl.col(column).cast(pl.Datetime(time_unit="us"))
    else:
        return pl.col(column)
        # raise RuntimeError("Unknown how to handle date type? " + schema[column] + " " + column)


def get_patient_link(
    person_df: pl.LazyFrame,
    death_df: pl.LazyFrame,
    visit_df: pl.LazyFrame,
    schema_loader: OMOPSchemaBase,
    limit: int = 0,
) -> pl.LazyFrame:
    """
    Process the persons table and death table to get an accurate birth and death datetime.
    Will produce a table of the subjects that will be included in the MEDS cohort.

    The output of this process is ultimately converted to events via the `patient` key in the
    `configs/event_configs.yaml` file.
        Args:
        person_df: A Polars LazyFrame containing person data.
        death_df: A Polars LazyFrame containing death data.
        visit_df: A Polars LazyFrame containing visit data.
        schema_loader: An instance of OMOPSchemaBase to load the schema.
        limit: An optional limit on the number of rows to process.

    Returns:
        A Polars LazyFrame with the processed patient data, including date of birth and date of death.

    Examples:
    >>> import polars as pl
    >>> from datetime import datetime
    >>> from omop_schema.utils import get_schema_loader
    >>> person_data = {
    ...     "person_id": [1, 2],
    ...     "year_of_birth": [1980, 1990],
    ...     "month_of_birth": [1, 2],
    ...     "day_of_birth": [1, 2],
    ...     "birth_datetime": [None, None]
    ... }
    >>> death_data = {
    ...     "person_id": [1],
    ...     "death_datetime": ["2020-01-01 00:00:00"]
    ... }
    >>> person_df = pl.DataFrame(person_data).lazy()
    >>> death_df = pl.DataFrame(death_data).lazy()
    >>> visit_df = pl.DataFrame({"person_id": [1, 2]}).lazy()
    >>> schema_loader = get_schema_loader(5.3)
    >>> result = get_patient_link(person_df, death_df, visit_df, schema_loader)
    >>> result_dict = result.collect().to_dict(as_series=False)  # Convert to plain Python dict
    """
    person_df = person_df.join(visit_df.select(SUBJECT_ID), on=SUBJECT_ID, how="semi")
    if limit > 0:
        # Limit the number of persons
        logger.info(f"Limiting the number of persons to {limit}")
        person_df = person_df.limit(limit)
    date_parsing = pl.datetime(
        pl.col("year_of_birth").replace(0, 1800).fill_null(1900),
        pl.col("month_of_birth").replace(0, 1).fill_null(1),
        pl.col("day_of_birth").replace(0, 1).fill_null(1),
        time_unit="us",
    )
    person_schema = person_df.collect_schema()
    if "birth_datetime" in person_schema:
        date_of_birth = (
            pl.when(pl.col("birth_datetime").is_not_null())
            .then(cast_to_datetime(person_df.collect_schema(), "birth_datetime"))
            .otherwise(date_parsing)
        )
    else:
        date_of_birth = date_parsing

    if death_df is None:
        death_schema = pyarrow_to_polars_schema(schema_loader.get_pyarrow_schema("death"))
        death_df = (
            pl.DataFrame(
                data=[],
                schema=death_schema,
            )
        ).lazy()
    date_of_death = pl.when(pl.col("death_datetime").is_not_null()).then(
        cast_to_datetime(death_df.collect_schema(), "death_datetime")
    )

    return (
        person_df.sort(by=date_of_birth)
        # .with_columns(pl.col(SUBJECT_ID))
        .group_by(SUBJECT_ID)
        .first()
        .join(death_df, on=SUBJECT_ID, how="left")  # Use renamed column
        .select(
            pl.col(SUBJECT_ID),
            date_of_birth.alias("date_of_birth"),
            # admission_time.alias("first_admitted_at_time"),
            date_of_death.alias("date_of_death"),
        )
        .with_columns(table_name=pl.lit("person"))
        .collect()
        .lazy()
    )  # We get parquet sink error if we don't collect here
    # visit_df,


def join_concept(
    table_name: str,
    reference_cols: str | list[str] | None = None,
    output_data_cols: list[str] | None = None,
    concept_cols: list[str] | None = None,
) -> Callable[[pl.LazyFrame, pl.LazyFrame], pl.LazyFrame]:
    """Returns a function that joins a dataframe to the `patient` table and adds pseudotimes.
    Also raises specified warning strings via the logger for uncertain columns.
    All args except `table_name` are taken from the table_preprocessors.yaml.
    Args:
        table_name: name of the table that should be joined
        output_data_cols: list of all data columns included in the output
        reference_cols: list of all columns that link to the concept_id
        concept_cols: list of all columns that are included in the concept table and
        should be added to the output
    Returns:
        Function that expects the raw data stored in the `table_name` table and the joined output of the
        `process_patient_and_admissions` function. Both inputs are expected to be `pl.DataFrame`s.

    Examples:
        >>> from omop_schema.utils import get_schema_loader
        >>> func = join_concept(
        ...     "observation",
        ...     ["observation_source_concept_id"],  # Add a comma here
        ...     ["observation_datetime", "observation_type_concept_id", "value_as_number", "value_as_string",
        ...     "value_as_concept_id", "qualifier_concept_id", "unit_concept_id", "visit_occurrence_id",
        ...      "visit_detail_id", "observation_source_value", "observation_source_concept_id"],
        ...     ["vocabulary_id", "concept_code"]
        ... )
        >>> schema_loader = get_schema_loader(5.3)
        >>> observation_df = load_raw_file(Path("tests/demo_resources/observation.csv"), schema_loader)
        >>> person_df = load_raw_file(Path("tests/demo_resources/person.csv"), schema_loader)
        >>> death_df = load_raw_file(Path("tests/demo_resources/death.csv"), schema_loader)
        >>> concept_df = load_raw_file(Path("tests/demo_resources/concept.csv"), schema_loader)
        >>> visit_df = load_raw_file(Path("tests/demo_resources/visit_occurrence.csv"), schema_loader)
        >>> patient_link = get_patient_link(person_df, death_df, visit_df, schema_loader)
        >>> processed_df = func(observation_df, concept_df, patient_link)
    """

    if output_data_cols is None:
        output_data_cols = []

    if reference_cols is None:
        reference_cols = []

    if concept_cols is None:
        concept_cols = []

    if isinstance(reference_cols, str):
        reference_cols = [reference_cols]

    def fn(df: pl.LazyFrame, concept_df: pl.LazyFrame, person_df: pl.LazyFrame) -> pl.LazyFrame:
        f"""Takes the {table_name} table and converts it to a form that includes the original concepts.

        The output of this process is ultimately converted to events via the `{table_name}` key in the
        `configs/event_configs.yaml` file.

        Args:
            df: The raw {table_name} data.
            concept_df: The concepts to join.
            person_df: The patient data to keep.

        Returns:
            The processed {table_name} data.
        """
        # logger.info(df.collect_schema())
        # # Join the patient table to the data table, INSPIRE only has subject_id as key
        # joined = df.join(patient_df.lazy(), on=ADMISSION_ID, how="inner")
        # collected = df.collect()
        df = df.with_columns(pl.col(SUBJECT_ID).cast(pl.Int64))
        # Keep only the persons that are in the patient table
        df = df.join(person_df, on=SUBJECT_ID, how="semi")
        if len(reference_cols) > 0:
            df = df.with_columns(pl.col(reference_cols).cast(pl.Int64))
            df = df.join(concept_df, left_on=reference_cols, right_on="concept_id", how="left")
        output_data_cols.extend(concept_cols)
        to_select = [col for col in output_data_cols if col in df.collect_schema().names()]
        to_select.append(SUBJECT_ID)
        # to_select.extend(concept_df.collect_schema().names())
        return df.select(to_select)  # .select(SUBJECT_ID, ADMISSION_ID, *output_data_cols)

    return fn


def load_raw_file(fp: Path, schema_loader: OMOPSchemaBase) -> pl.LazyFrame | None:
    """Retrieve all .csv/.csv.gz/.parquet files for the OMOP table given by fp

    Because OMOP tables can be quite large for datasets comprising millions
    of subjects, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. This function
    takes a path corresponding to an OMOP table with its standard name (e.g.,
    `condition_occurrence`, `measurement`, `observation`) and returns two list
    of paths.

    The first list contains all the csv files. The second list contains all parquet files.

    Examples:
        >>> from pathlib import Path
        >>> import polars as pl
        >>> from omop_schema.utils import get_schema_loader
        >>> schema_loader = get_schema_loader(5.3)
        >>> fp = Path("tests/demo_resources/observation.csv")
        >>> df = load_raw_file(fp, schema_loader)
    """
    # TODO: Write tool/method that reads a specific omop table with a specific datatypes
    table_name = fp.stem.split(".")[0]  # Infer table name from file path
    schema = pyarrow_to_polars_schema(schema_loader.get_pyarrow_schema(table_name))

    # pa.schema([(col, dtype) for coll, dtype in schema.items()])
    # Convert dict to pa.Schema
    if fp.suffixes == [".csv", ".gz"]:
        file = pl.scan_csv(fp, compression="gzip", infer_schema=False, schema_overrides=schema)
    elif fp.suffix == ".csv":
        # Using schema_overrides to set the schema as the ordering could be different
        # and there could be extra columns
        file = pl.scan_csv(fp, infer_schema=False, has_header=True, schema_overrides=schema)
    elif fp.suffix == ".parquet":
        file = pl.scan_parquet(fp)  # , schema=schema, allow_missing_columns=True)
        file = file.select(pl.all().name.to_lowercase())
        file = convert_to_schema_polars(file, schema, allow_extra_columns=True)
    elif fp.is_dir():
        files = list(fp.glob("**/*"))
        csv_files = [file for file in files if file.suffix in [".csv", ".gz"]]
        parquet_files = [file for file in files if file.suffix == ".parquet"]
        if csv_files:
            file = pl.scan_csv(fp, infer_schema=False, has_header=True, schema_overrides=schema)
        elif parquet_files:
            file = pl.scan_parquet(fp)  # , schema=schema, allow_missing_columns=True)
            file = file.select(pl.all().name.to_lowercase())
            file = convert_to_schema_polars(file, schema, allow_extra_columns=True)
        else:
            return None
    else:
        return None
    file = file.select(pl.all().name.to_lowercase())
    return file


def extract_metadata(concept_df: pl.LazyFrame, concept_relationship_df: pl.LazyFrame) -> pl.LazyFrame:
    # concept_id_map: Dict[int, str] = {}  # [key] concept_id -> [value] concept_code
    # concept_name_map: Dict[int, str] = {}  # [key] concept_id -> [value] concept_name
    # code_metadata: Dict[str, Any] = {}  # [key] concept_code -> [value] metadata

    # Read in the OMOP `CONCEPT` table from disk
    # (see https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html#concepts)
    # and use it to generate metadata file as well as populate maps
    # from (concept ID -> concept code) and (concept ID -> concept name)
    # for concept_file in tqdm(itertools.chain(*get_table_files(path_to_src_omop_dir, "concept")),
    #                          total=len(get_table_files(path_to_src_omop_dir, "concept")[0]) +
    #                          len(get_table_files(path_to_src_omop_dir, "concept")[1]),
    #                          desc="Generating metadata from OMOP `concept` table"):
    #     # Note: Concept table is often split into gzipped shards by default
    #     if verbose:
    #         print(concept_file)
    #     with load_file(path_to_decompressed_dir, concept_file) as f:
    # Read the contents of the `concept` table shard
    # `load_file` will unzip the file into `path_to_decompressed_dir` if needed
    logger.info("Generating codes metadata from OMOP `concept` table and `concept_relationship` table")
    concept = concept_df
    concept_id = pl.col("concept_id").cast(pl.Int64)
    code = pl.col("vocabulary_id") + "/" + pl.col("concept_code")
    logger.info(concept.collect_schema())
    # Convert the table into a dictionary
    result = concept.select(concept_id=concept_id, code=code, name=pl.col("concept_name"))
    concept_relationship_df = concept_relationship_df.with_columns(
        pl.col("concept_id_1").cast(pl.Int64), pl.col("concept_id_2").cast(pl.Int64)
    )
    # Take the parents of the concepts
    parent_codes = concept_relationship_df.filter(pl.col("relationship_id") == "Maps to")
    parent_codes = parent_codes.with_columns(
        pl.col("concept_id_2").alias("parent_codes").cast(pl.List(pl.Int64))
    )
    result = result.join(parent_codes, left_on="concept_id", right_on="concept_id_1", how="left")
    code_metadata = result.select("code", "name", "parent_codes")

    # result = result.to_dict(as_series=False)

    # Update our running dictionary with the concepts we read in from
    # the concept table shard
    # concept_id_map |= dict(zip(result["concept_id"], result["code"]))
    # concept_name_map |= dict(zip(result["concept_id"], result["name"]))

    # Assuming custom concepts have concept_id > 2000000000 we create a
    # record for them in `code_metadata` with no parent codes. Such a
    # custom code could be eg `STANFORD_RACE/Black or African American`
    # with `concept_id` 2000039197
    # custom_concepts = (
    #     concept.filter(concept_id > CUSTOMER_CONCEPT_ID_START)
    #     .select(concept_id=concept_id, code=code, description=pl.col("concept_name"))
    #     .to_dict()
    # )
    # for i in range(len(custom_concepts["code"])):
    #     code_metadata[custom_concepts["code"][i]] = {
    #         "code": custom_concepts["code"][i],
    #         "description": custom_concepts["description"][i],
    #         "parent_codes": [],
    #     }

    # Include map from custom concepts to normalized (ie standard ontology)
    # parent concepts, where possible, in the code_metadata dictionary
    # for concept_relationship_file in tqdm(itertools.chain(
    # *get_table_files(path_to_src_omop_dir, "concept_relationship")),
    #   total=len(get_table_files(path_to_src_omop_dir, "concept_relationship")[0]) +
    #   len(get_table_files(path_to_src_omop_dir, "concept_relationship")[1]),
    #   desc="Generating metadata from OMOP `concept_relationship` table"):
    #     with load_file(path_to_decompressed_dir, concept_relationship_file) as f:
    # This table has `concept_id_1`, `concept_id_2`, `relationship_id` columns

    # custom_relationships = (
    #     concept_relationship.filter(
    #         concept_id_1 > CUSTOMER_CONCEPT_ID_START,
    #         pl.col("relationship_id") == "Maps to",
    #         concept_id_1 != concept_id_2,
    #     )
    #     .select(concept_id_1=concept_id_1, concept_id_2=concept_id_2)
    #     .to_dict(as_series=False)
    # )

    # for concept_id_1, concept_id_2 in zip(
    #     custom_relationships["concept_id_1"], custom_relationships["concept_id_2"]
    # ):
    #     if concept_id_1 in concept_id_map and concept_id_2 in concept_id_map:
    #         code_metadata[concept_id_map[concept_id_1]]["parent_codes"].append(concept_id_map[concept_id_2])
    return code_metadata  # concept_id_map, concept_name_map


def rename_demo_files(directory: Path):
    """Rename files in the directory by removing the '2b_' prefix."""
    for file_path in directory.glob("2b_*"):
        new_name = file_path.name.replace("2b_", "")
        new_path = file_path.with_name(new_name)
        file_path.rename(new_path)
        logger.info(f"Renamed: {file_path} to {new_path}")
