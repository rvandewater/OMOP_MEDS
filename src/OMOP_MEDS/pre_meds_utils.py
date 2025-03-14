from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

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


def get_patient_link(person_df: pl.LazyFrame, death_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Process the operations table to get the patient table and the link table.

    As dataset may store only offset times, note here that we add a CONSTANT TIME ACROSS ALL PATIENTS for the
    true timestamp of their health system admission.

    The output of this process is ultimately converted to events via the `patient` key in the
    `configs/event_configs.yaml` file.
    """
    # admission_time = pl.datetime()
    # age_in_years = pl.col()
    # age_in_days = age_in_years * 365.25
    #
    # pseudo_date_of_birth = admission_time - pl.duration(days=age_in_days)
    # pseudo_date_of_death = admission_time + pl.duration(seconds=pl.col())

    # date_of_birth =  pl.coalesce(
    #             pl.col( "birth_datetime"),
    #             pl.datetime(
    #                 pl.col("year_of_birth"),
    #                 pl.coalesce(pl.col("month_of_birth"), pl.lit(1)),
    #                 pl.coalesce(pl.col("day_of_birth"), pl.lit(1)),
    #                 time_unit="us",
    #             ),
    #         )
    # person_df.filter()
    date_parsing = (
                pl.datetime(
                    pl.col("year_of_birth").replace(0, 1800).fill_null(1900),
                    pl.col("month_of_birth").replace(0, 1).fill_null(1),
                    pl.col("day_of_birth").replace(0, 1).fill_null(1),
                    time_unit="us",
                )
            )
    person_schema = person_df.collect_schema()
    if "birth_datetime" in person_schema:
        date_of_birth = (
            pl.when(pl.col("birth_datetime").is_not_null())
            .then(cast_to_datetime(person_df.collect_schema(), "birth_datetime"))
            .otherwise(date_parsing
            )
        )
    else:
        date_of_birth = date_parsing
    # admission_time = pl.col("admission_time")
    # date_of_death = pl.col("death_datetime")
    if death_df is not None:

        death_df = death_df.with_columns(pl.col(SUBJECT_ID).cast(pl.Int64))
    else:
        death_df = (pl.DataFrame(
            data=[],
            schema={
            SUBJECT_ID: pl.Int64,
            "date_of_death": pl.Datetime,
            "death_datetime": pl.Datetime,
        })).lazy()
    date_of_death = pl.when(pl.col("death_datetime").is_not_null()).then(
        cast_to_datetime(death_df.collect_schema(), "death_datetime")
    )
    # TODO: join with location, provider, care_site,
    return (
        person_df.sort(by=date_of_birth)
        .with_columns(pl.col(SUBJECT_ID).cast(pl.Int64))
        .group_by(SUBJECT_ID)
        .first()
        .join(death_df, on=SUBJECT_ID, how="left")
        .select(
            SUBJECT_ID,
            date_of_birth.alias("date_of_birth"),
            # admission_time.alias("first_admitted_at_time"),
            date_of_death.alias("date_of_death"),
        )
        .with_columns(table_name=pl.lit("person"))
        .collect()
        .lazy()
    )  # We get parquet sink error if we don't collect here
    # visit_df,


def join_concept_and_process_psuedotime(
    table_name: str,
    offset_col: str | list[str] | None = None,
    pseudotime_col: str | list[str] | None = None,
    reference_col: str | list[str] | None = None,
    output_data_cols: list[str] | None = None,
    concept_cols: list[str] | None = None,
    warning_items: list[str] | None = None,
) -> Callable[[pl.LazyFrame, pl.LazyFrame], pl.LazyFrame]:
    """Returns a function that joins a dataframe to the `patient` table and adds pseudotimes.
    Also raises specified warning strings via the logger for uncertain columns.
    All args except `table_name` are taken from the table_preprocessors.yaml.
    Args:
        table_name: name of the INSPIRE table that should be joined
        offset_col: list of all columns that contain time offsets since the patient's first admission
        pseudotime_col: list of all timestamp columns derived from `offset_col` and the linked `patient`
            table
        output_data_cols: list of all data columns included in the output
        warning_items: any warnings noted in the table_preprocessors.yaml

    Returns:
        Function that expects the raw data stored in the `table_name` table and the joined output of the
        `process_patient_and_admissions` function. Both inputs are expected to be `pl.DataFrame`s.

    Examples:
        >>> func = join_concept_and_process_psuedotime(
        ...     "operations",
        ...     ["admission_time", "icuin_time", "icuout_time", "orin_time", "orout_time",
        ...      "opstart_time", "opend_time", "discharge_time", "anstart_time", "anend_time",
        ...      "cpbon_time", "cpboff_time", "inhosp_death_time", "allcause_death_time", "opdate"],
        ...     ["admission_time", "icuin_time", "icuout_time", "orin_time", "orout_time",
        ...      "opstart_time", "opend_time", "discharge_time", "anstart_time", "anend_time",
        ...      "cpbon_time", "cpboff_time", "inhosp_death_time", "allcause_death_time", "opdate"],
        ...     ["subject_id", "op_id", "age", "antype", "sex", "weight", "height", "race", "asa",
        ...      "case_id", "hadm_id", "department", "emop", "icd10_pcs", "date_of_birth",
        ...      "date_of_death"],
        ...     ["How should we deal with op_id and subject_id?"]
        ... )
        >>> df = load_raw_file(Path("tests/operations_synthetic.csv"))
        >>> raw_admissions_df = load_raw_file(Path("tests/operations_synthetic.csv"))
        >>> patient_df, link_df = get_patient_link(raw_admissions_df)
        >>> references_df = load_raw_file(Path("tests/d_references.csv"))
        >>> processed_df = func(df, patient_df, references_df)
        >>> type(processed_df)
        >>> <class 'polars.lazyframe.frame.LazyFrame'>
    """

    if output_data_cols is None:
        output_data_cols = []

    if reference_col is None:
        reference_col = []

    if offset_col is None:
        offset_col = []

    if pseudotime_col is None:
        pseudotime_col = []

    if concept_cols is None:
        concept_cols = []

    if isinstance(offset_col, str):
        offset_col = [offset_col]
    if isinstance(pseudotime_col, str):
        pseudotime_col = [pseudotime_col]
    if isinstance(reference_col, str):
        reference_col = [reference_col]

    if len(offset_col) != len(pseudotime_col):
        raise ValueError(
            "There must be the same number of `offset_col`s and `pseudotime_col`s specified. Got "
            f"{len(offset_col)} and {len(pseudotime_col)}, respectively."
        )
    if set(offset_col) & set(output_data_cols) or set(pseudotime_col) & set(output_data_cols):
        raise ValueError(
            "There is an overlap between `offset_col` or `pseudotime_col` and `output_data_cols`: "
            f"{set(offset_col) & set(output_data_cols) | set(pseudotime_col) & set(output_data_cols)}"
        )

    def fn(df: pl.LazyFrame, references_df: pl.LazyFrame) -> pl.LazyFrame:
        f"""Takes the {table_name} table and converts it to a form that includes pseudo-timestamps.

        The output of this process is ultimately converted to events via the `{table_name}` key in the
        `configs/event_configs.yaml` file.

        Args:
            df: The raw {table_name} data.
            patient_df: The processed patient data.

        Returns:
            The processed {table_name} data.
        """
        # pseudotimes = [
        #     (pl.col("first_admitted_at_time") + pl.duration(seconds=pl.col(offset))).alias(pseudotime)
        #     for pseudotime, offset in zip(pseudotime_col, offset_col)
        # ]
        # if warning_items:
        #     warning_lines = [
        #         f"NOT SURE ABOUT THE FOLLOWING for {table_name} table. Check with the {DATASET_NAME} team:",
        #         *(f"  - {item}" for item in warning_items),
        #     ]
        #     logger.warning("\n".join(warning_lines))
        # logger.info(f"Joining {table_name} to patient table...")
        # logger.info(df.collect_schema())
        # # Join the patient table to the data table, INSPIRE only has subject_id as key
        # joined = df.join(patient_df.lazy(), on=ADMISSION_ID, how="inner")
        # collected = df.collect()
        df = df.with_columns(pl.col(SUBJECT_ID).cast(pl.Int64))
        if len(reference_col) > 0:
            df = df.join(references_df, left_on=reference_col, right_on="concept_id", how="left")
        # collected = joined.collect()
        return df  # .select(SUBJECT_ID, ADMISSION_ID, *output_data_cols)

    return fn


def load_raw_file(fp: Path) -> pl.LazyFrame:
    """Retrieve all .csv/.csv.gz/.parquet files for the OMOP table given by fp

    Because OMOP tables can be quite large for datasets comprising millions
    of subjects, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. This function
    takes a path corresponding to an OMOP table with its standard name (e.g.,
    `condition_occurrence`, `measurement`, `observation`) and returns two list
    of paths.

    The first list contains all the csv files. The second list contains all parquet files.
    """
    if fp.suffixes == [".csv", ".gz"]:
        file = pl.scan_csv(fp, compression="gzip", infer_schema=False)
    elif fp.suffix == ".csv":
        file = pl.scan_csv(fp, infer_schema=False)
    elif fp.suffix == ".parquet":
        file = pl.scan_parquet(fp)
    elif fp.is_dir():
        files = list(fp.glob("**/*"))
        csv_files = [file for file in files if file.suffix in [".csv", ".gz"]]
        parquet_files = [file for file in files if file.suffix == ".parquet"]
        if csv_files:
            file = pl.scan_csv(csv_files, infer_schema=False)
        elif parquet_files:
            file = pl.scan_parquet(parquet_files)
    else:
        return None
    file = file.select(pl.all().name.to_lowercase())
    return file
        #raise ValueError(f"Unknown file type for {fp}")
    # if os.path.exists(path_to_table) and os.path.isdir(path_to_table):
    #     csv_files = []
    #     parquet_files = []
    #
    #     for a in os.listdir(path_to_table):
    #         fname = os.path.join(path_to_table, a)
    #         if a.endswith(".csv") or a.endswith(".csv.gz"):
    #             csv_files.append(fname)
    #         elif a.endswith(".parquet"):
    #             parquet_files.append(fname)
    #
    #     return csv_files, parquet_files
    # elif os.path.exists(path_to_table + ".csv"):
    #     return pl.read_parque(path_to_table + ".csv"
    # elif os.path.exists(path_to_table + ".csv.gz"):
    #     return path_to_table + ".csv.gz"
    # elif os.path.exists(path_to_table + ".parquet"):
    #     return path_to_table + ".parquet"
    # else:
    #     raise


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

