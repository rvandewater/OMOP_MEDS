import logging
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
        [
            time.str.to_datetime(time_format, strict=False, time_unit="us")
            for time_format in time_formats
        ]
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
                time.str.to_datetime(
                    "%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="us"
                ),
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
        death_schema = pyarrow_to_polars_schema(
            schema_loader.get_pyarrow_schema("death")
        )
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


def join_concept(
    table_name: str,
    reference_cols: str | list[str] | None = None,
    output_data_cols: list[str] | None = None,
    concept_cols: list[str] | None = None,
    prefer_source: bool = False,
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
        prefer_source: If True, prefer the source concept over the mapped concept.
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
        ...     ["vocabulary_id", "concept_code"],
        ...     prefer_source=False,
        ...
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

    def fn(
        df: pl.LazyFrame, concept_df: pl.LazyFrame, person_df: pl.LazyFrame
    ) -> pl.LazyFrame:
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
        # df = df.with_columns("preferred_concept_name", pl.lit(None))
        # df = df.with_columns("preferred_vocabulary_name", pl.lit(None))
        # Keep only the persons that are in the patient table
        df = df.join(person_df, on=SUBJECT_ID, how="semi")
        if len(reference_cols) > 0:
            df = df.with_columns(pl.col(reference_cols).cast(pl.Int64).replace(0, None))
            if len(reference_cols) == 1:
                df = df.join(
                    concept_df,
                    left_on=reference_cols,
                    right_on="concept_id",
                    how="left",
                )
                df = df.with_columns(
                    pl.col(reference_cols).alias("preferred_concept_name"),
                    pl.col("vocabulary_id").alias("preferred_vocabulary_name"),
                )
            else:
                clean_item = ""
                for item in reference_cols:
                    table_names = table_name.split("_")
                    clean_item = item
                    for part in table_names:
                        clean_item = clean_item.replace(part, "")
                    clean_item = clean_item.lstrip("_")
                    # Remove the table name prefix
                    df = df.join(
                        concept_df,
                        left_on=item,
                        right_on="concept_id",
                        how="left",
                        suffix=f"_{clean_item}",
                    )
                # Determine the concept id for the codes
                df = determine_concept_id(
                    df,
                    original_concept_id_cols=reference_cols,
                    mapped_concept_col="concept_code",
                    mapped_vocab_col="vocabulary_id",
                    source_concept_col=f"concept_code_{clean_item}",
                    source_vocab_col=f"vocabulary_id_{clean_item}",
                    prefer_source=prefer_source,
                )
        # df_collected = df.collect()
        output_data_cols.extend(concept_cols)
        to_select = [
            col for col in output_data_cols if col in df.collect_schema().names()
        ]
        to_select.append(SUBJECT_ID)
        if "preferred_concept_name" in df.collect_schema().names():
            to_select.extend(["preferred_concept_name", "preferred_vocabulary_name"])
            df = df.with_columns(
                pl.col("preferred_vocabulary_name").replace(None, f"OMOP_{table_name}")
            )
        return df.select(to_select)

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
        file = pl.scan_csv(
            fp, compression="gzip", infer_schema=False, schema_overrides=schema
        )
        logging.info(f"Loaded gzipped CSV file from {fp}")
    elif fp.suffix == ".csv":
        # Using schema_overrides to set the schema as the ordering could be different
        # and there could be extra columns
        file = pl.scan_csv(
            fp, infer_schema=False, has_header=True, schema_overrides=schema
        )
        logging.info(f"Loaded CSV file from {fp}")
    elif fp.suffix == ".parquet":
        file = pl.scan_parquet(fp)  # , schema=schema, allow_missing_columns=True)
        file = file.select(pl.all().name.to_lowercase())
        file = convert_to_schema_polars(file, schema, allow_extra_columns=True)
        logging.info(f"Loaded Parquet file from {fp}")
    elif fp.is_dir():
        files = list(fp.glob("**/*"))
        csv_files = [file for file in files if file.suffix in [".csv", ".gz"]]
        parquet_files = [file for file in files if file.suffix == ".parquet"]
        # mismatching_schema_check = True
        if csv_files:
            file = pl.scan_csv(
                fp, infer_schema=False, has_header=True, schema_overrides=schema
            )
            logging.info(f"Loaded CSV files as directory from {fp}")
        elif parquet_files:
            file = pl.scan_parquet(fp)  # , schema=schema, allow_missing_columns=True)
            file = file.select(pl.all().name.to_lowercase())
            # if mismatching_schema_check:
            #     cast_files_to_schema(str(fp), schema, str(fp))
            file = convert_to_schema_polars(file, schema, allow_extra_columns=False)
            logging.info(f"Loaded Parquet files as directory from {fp}")
        else:
            return None
    else:
        return None
    file = file.select(pl.all().name.to_lowercase())
    return file


def cast_files_to_schema(folder_path: str, target_schema: dict, output_folder: str):
    """
    Casts all files in a folder to the target schema using convert_to_schema_polars.
    Used when the schema of the files in the folder is inconsistent.

    Args:
        folder_path (str): Path to the folder containing the files.
        target_schema (dict): The target schema (column name -> data type).
        output_folder (str): Path to the folder where processed files will be saved.

    Returns:
        None
    """
    folder = Path(folder_path)
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check for column mismatches
    mismatched_columns = check_column_mismatches(folder_path)

    if mismatched_columns:
        print(f"Found mismatched columns: {mismatched_columns}")

    # Process each file in the folder
    for file in folder.glob("*.parquet"):
        print(f"Processing file: {file}")
        df = pl.scan_parquet(file)

        # Align the file's schema with the target schema
        df = convert_to_schema_polars(
            dataset=df.lazy(),
            target_schema=target_schema,
            allow_extra_columns=False,
            allow_missing_columns=True,
            add_missing_columns=True,
        ).collect()

        # Save the processed file to the output folder
        output_file = output_dir / file.name
        df.write_parquet(output_file)
        print(f"File saved to: {output_file}")


def check_column_mismatches(folder_path: str):
    """
    Check for column mismatches (e.g., datatype inconsistencies) across all Parquet files in a folder.

    Parameters:
    folder_path (str): The path to the folder containing Parquet files.

    Returns:
    dict: A dictionary where keys are column names and values are lists of inconsistent datatypes.
    """
    column_types = {}

    # Iterate through all Parquet files in the folder
    for file in Path(folder_path).glob("*.parquet"):
        print(f"Checking file: {file}")
        # Read the schema of the Parquet file
        df = pl.read_parquet(file)
        schema = df.schema

        # Compare the schema with the accumulated column types
        for col, dtype in schema.items():
            if col not in column_types:
                column_types[col] = set()
            column_types[col].add(dtype)

    # Identify mismatched columns
    mismatched_columns = {
        col: list(dtypes) for col, dtypes in column_types.items() if len(dtypes) > 1
    }

    if mismatched_columns:
        print("Mismatched columns found:")
        for col, dtypes in mismatched_columns.items():
            print(f"  Column: {col}, Datatypes: {dtypes}")
    else:
        print("No mismatched columns found. All schemas are consistent.")

    return mismatched_columns


def extract_metadata(
    concept_df: pl.LazyFrame, concept_relationship_df: pl.LazyFrame
) -> pl.LazyFrame:
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
    logger.info(
        "Generating codes metadata from OMOP `concept` table and `concept_relationship` table"
    )
    concept = concept_df
    concept_id = pl.col("concept_id").cast(pl.Int64)
    # code = pl.col("vocabulary_id") + "//" + pl.col("concept_code")
    logger.info(concept.collect_schema())
    # Convert the table into a dictionary
    result = concept.select(
        concept_id=concept_id,
        vocabulary_id=pl.col("vocabulary_id"),
        description=pl.col("concept_name"),
    )
    concept_relationship_df = concept_relationship_df.with_columns(
        pl.col("concept_id_1").cast(pl.Int64), pl.col("concept_id_2").cast(pl.Int64)
    )

    # Take the parents of the concepts
    parent_codes = concept_relationship_df.filter(
        pl.col("relationship_id") == "Maps to"
    )
    parent_codes = parent_codes.join(
        concept_df, left_on="concept_id_2", right_on="concept_id", how="left"
    )
    parent_codes = parent_codes.with_columns(
        parent_codes=pl.col("vocabulary_id") + "//" + pl.col("concept_code")
    )
    parent_codes = parent_codes.with_columns(
        parent_codes=pl.col("parent_codes").cast(pl.List(pl.String))
    )
    result = result.join(
        parent_codes, left_on="concept_id", right_on="concept_id_1", how="left"
    )
    # code_metadata = result
    code_metadata = result.with_columns(
        code=pl.col("vocabulary_id").cast(pl.Utf8)
        + "//"
        + pl.col("concept_id").cast(pl.Utf8)
    )
    code_metadata = code_metadata.select(
        "code", "vocabulary_id", "concept_id", "description", "parent_codes"
    )

    # code_metadata = code_metadata.with_columns(pl.col("name").alias("description"))
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


def determine_concept_id(
    df: pl.LazyFrame,
    original_concept_id_cols: list[str],
    mapped_concept_col: str,
    mapped_vocab_col: str,
    source_concept_col: str,
    source_vocab_col: str,
    prefer_source: bool = False,
) -> pl.LazyFrame:
    """
    Determines the concept ID and vocabulary ID for a given DataFrame by prioritizing
    either the source or mapped concept columns based on the `prefer_source` flag.

    Args:
        df (pl.LazyFrame): The input DataFrame containing concept-related columns.
        original_concept_id_cols (list[str]): List of original concept ID columns to consider.
        mapped_concept_col (str): Column name for the mapped concept ID.
        mapped_vocab_col (str): Column name for the mapped vocabulary ID.
        source_concept_col (str): Column name for the source concept ID.
        source_vocab_col (str): Column name for the source vocabulary ID.
        prefer_source (bool, optional): If `True`, prioritizes the source concept and vocabulary
            columns over the mapped ones. Defaults to `False`.

    Returns:
        pl.LazyFrame: A new DataFrame with the determined concept ID and vocabulary ID
        added as `preferred_concept_name` and `preferred_vocabulary_name` columns.

    Example:
        >>> df = pl.LazyFrame({"mapped_concept_col": [1, None], "source_concept_col": [None, 2]})
        >>> result = determine_concept_id(
        ...     df,
        ...     original_concept_id_cols=["original_col"],
        ...     mapped_concept_col="mapped_concept_col",
        ...     mapped_vocab_col="mapped_vocab_col",
        ...     source_concept_col="source_concept_col",
        ...     source_vocab_col="source_vocab_col",
        ...     prefer_source=True
        ... )
    """
    if prefer_source:
        vocab_id = (
            pl.when(
                pl.col(source_concept_col).is_not_null()
                & pl.col(source_vocab_col).is_not_null()
            )
            .then(pl.col(source_vocab_col))
            .otherwise(
                pl.when(
                    pl.col(mapped_concept_col).is_not_null()
                    & pl.col(mapped_vocab_col).is_not_null()
                )
                .then(pl.col(mapped_vocab_col))
                .otherwise(
                    pl.lit(None)
                )  # Default to None if no valid vocab_id is found
            )
        )

        concept_id = (
            pl.when(pl.col(source_concept_col).is_not_null())
            .then(pl.col(source_concept_col))
            .otherwise(
                pl.when(pl.col(mapped_concept_col).is_not_null())
                .then(pl.col(mapped_concept_col))
                .otherwise(
                    pl.when(pl.col(original_concept_id_cols[0]).is_not_null())
                    .then(
                        original_concept_id_cols[0]
                        + ":"
                        + pl.concat_str(original_concept_id_cols[0], separator=",")
                    )
                    .otherwise(
                        pl.lit(None)
                    )  # Default to None if no valid concept_id is found
                )
            )
        )
    else:
        vocab_id = (
            pl.when(
                pl.col(mapped_vocab_col).is_not_null()
                & pl.col(mapped_concept_col).is_not_null()
            )
            .then(pl.col(mapped_vocab_col))
            .otherwise(
                pl.when(
                    pl.col(source_vocab_col).is_not_null()
                    & pl.col(source_concept_col).is_not_null()
                )
                .then(pl.col(source_vocab_col))
                .otherwise(
                    pl.lit(None)
                )  # Default to None if no valid vocab_id is found
            )
        )

        concept_id = (
            pl.when(pl.col(mapped_concept_col).is_not_null())
            .then(pl.col(mapped_concept_col))
            .otherwise(
                pl.when(pl.col(source_concept_col).is_not_null())
                .then(pl.col(source_concept_col))
                .otherwise(
                    pl.when(pl.col(original_concept_id_cols[0]).is_not_null())
                    .then(
                        pl.concat_str(
                            [
                                original_concept_id_cols[0]
                                + ":"
                                + pl.concat_str(original_concept_id_cols, separator=",")
                            ]
                        )
                    )
                    # .then("".join([f"{col} = {pl.col(col)}" for col in original_concept_id_cols]))
                    # .then(pl.when(len(list(original_concept_id_cols))> 1 &
                    # pl.col(original_concept_id_cols[1])
                    # .is_not_null())
                    #     .then(original_concept_id_cols[0] +":" +
                    #           pl.concat_str(original_concept_id_cols, separator=f",
                    #           {original_concept_id_cols[1]}"))
                    #     .otherwise(original_concept_id_cols[0] + pl.concat_str(original_concept_id_cols,
                    #     separator=",")))
                    .otherwise(
                        pl.lit(None)
                    )  # Default to None if no valid concept_id is found
                )
            )
        )
    df = df.with_columns(
        concept_id.alias("preferred_concept_name"),
        vocab_id.alias("preferred_vocabulary_name"),
    )
    return df


def rename_demo_files(directory: Path):
    """Rename files in the directory by removing the '2b_' prefix in the MIMIC-OMOP demo."""
    for file_path in directory.glob("2b_*"):
        new_name = file_path.name.replace("2b_", "")
        new_path = file_path.with_name(new_name)
        file_path.rename(new_path)
        logger.info(f"Renamed: {file_path} to {new_path}")
