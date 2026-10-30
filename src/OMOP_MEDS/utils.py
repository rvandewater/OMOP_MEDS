import logging
import os
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from dftly import Parser
from meds import CodeMetadataSchema
from omegaconf import OmegaConf

logger = logging.getLogger(__name__)

# polars writes strings as Arrow `large_string`, which `meds.CodeMetadataSchema` rejects for `code`,
# `description` and `parent_codes`; all string columns are written as `string`, as MEDS-Extract does.
ARROW_STRING_TYPES = {
    pa.large_string(): pa.string(),
    pa.list_(pa.large_string()): pa.list_(pa.string()),
    pa.large_list(pa.large_string()): pa.list_(pa.string()),
}
CODES_METADATA_COLUMNS = [
    "code",
    "table_name",
    "occurrence_count",
    "base_code",
    "vocabulary_id",
    "concept_id",
    "concept_code",
    "description",
    "parent_codes",
]


def finish_codes_metadata(
    MEDS_cohort_dir: Path, pre_MEDS_dir: Path, event_cfg_path: Path | None = None
) -> Path | None:
    """Writes `metadata/codes.parquet`: every code in the MEDS data with its OMOP concept's metadata.

    Each code is linked to the OMOP concept it was built from (see `code_concept_links`) and gets that
    concept's `concept_name` as description and, as parent codes, the `vocabulary_id//concept_code` of
    every concept it "Maps to" in the source `concept_relationship` table (`extract_codes_metadata`).
    Codes are listed once per table they occur in, with their occurrence count.

    Returns:
        The path of the written file, or None when the pre-MEDS concept metadata is missing.
    """
    codes_source = pre_MEDS_dir / "codes.parquet"
    codes_dest = MEDS_cohort_dir / "metadata/codes.parquet"
    if not codes_source.exists():
        logger.warning(f"codes.parquet not found in {pre_MEDS_dir}")
        return None
    if event_cfg_path is None:
        from . import EVENT_CFG

        event_cfg_path = EVENT_CFG

    logger.info(f"Writing code metadata from {codes_source} to {codes_dest}")
    collected = (
        pl.scan_parquet(MEDS_cohort_dir / "data/**/*.parquet")
        .select(["code", "table_name"])
        .group_by(["code", "table_name"])
        .agg(pl.len().alias("occurrence_count"))
        .collect()
        .with_columns(
            pl.col("code").str.replace(r"(//start|//end)$", "").alias("base_code")
        )
    )
    links = code_concept_links(pre_MEDS_dir, OmegaConf.load(event_cfg_path))
    table_key = pl.col("table_name").fill_null("")
    merged = (
        collected.with_columns(table_key.alias("_table_key"))
        .join(
            links.with_columns(table_key.alias("_table_key")).drop("table_name"),
            on=["code", "_table_key"],
            how="left",
        )
        .join(_concept_metadata(codes_source), on="concept_id", how="left")
        .with_columns(_drop_self_parent(pl.col("parent_codes")))
        .select(CODES_METADATA_COLUMNS)
    )
    write_codes_metadata(merged, codes_dest)
    logger.info(
        f"Code metadata: {merged.height} codes, {merged['concept_id'].is_not_null().sum()} linked to a "
        f"concept, {merged['description'].is_not_null().sum()} described, "
        f"{merged['parent_codes'].is_not_null().sum()} with parent codes."
    )
    return codes_dest


def code_concept_links(pre_MEDS_dir: Path, event_cfg) -> pl.DataFrame:
    """Returns the OMOP concept each event code stands for, as `code`, `table_name`, `concept_id`.

    The codes are rendered with the event config's own code templates on the pre-MEDS tables, so the link
    holds whatever the template's concept component is: `preferred_concept_name`, which is a
    `concept_code` for tables joined on a mapped and a source concept but a `concept_id` for tables
    joined on one (its concept is the pre-MEDS `preferred_concept_id`), or a raw `*_concept_id` column.
    Joining the metadata on the code string instead would key both kinds by `concept_id` and miss, or
    mislabel, every `concept_code` code. Templates without a concept column (e.g. `GENDER//{$gender}`) and
    literal codes are not linked.

    Examples:
        >>> import tempfile
        >>> event_cfg = {
        ...     "subject_id_col": "person_id",
        ...     "measurement": {"measurement": {
        ...         "code": 'f"{$preferred_vocabulary_name}//{$preferred_concept_name}"',
        ...         "time": "$measurement_datetime", "table_name": "$table_name"}},
        ...     "drug_era": {"drug_era": {
        ...         "code": 'f"{$preferred_vocabulary_name}//{$preferred_concept_name}//start"',
        ...         "time": "$drug_era_start_date", "table_name": "$table_name"}},
        ...     "person_birth_death": {"death": {"code": "MEDS_DEATH", "time": "$date_of_death"}},
        ... }
        >>> with tempfile.TemporaryDirectory() as d:
        ...     pl.DataFrame({
        ...         "preferred_vocabulary_name": ["LOINC", "OMOP_measurement"],
        ...         "preferred_concept_name": ["718-7", "measurement_concept_id:3000963,0"],
        ...         "preferred_concept_id": [3000963, 3000963],
        ...         "table_name": ["measurement", "measurement"],
        ...     }).write_parquet(Path(d) / "measurement.parquet")
        ...     pl.DataFrame({
        ...         "preferred_vocabulary_name": ["RxNorm"],
        ...         "preferred_concept_name": [1125315],
        ...         "preferred_concept_id": [1125315],
        ...         "table_name": ["drug_era"],
        ...     }).write_parquet(Path(d) / "drug_era.parquet")
        ...     code_concept_links(Path(d), event_cfg).sort("code").rows()
        [('LOINC//718-7', 'measurement', 3000963), ('OMOP_measurement//measurement_concept_id:3000963,0', 'measurement', 3000963), ('RxNorm//1125315//start', 'drug_era', 1125315)]
    """
    links = []
    for table, events in event_cfg.items():
        if not hasattr(events, "items"):
            continue
        fp = pre_MEDS_dir / f"{table}.parquet"
        if not fp.exists():
            continue
        df = pl.scan_parquet(fp / "*.parquet" if fp.is_dir() else fp)
        columns = set(df.collect_schema().names())
        for event_name, event in events.items():
            if not hasattr(event, "get") or event.get("code") is None:
                continue
            code = Parser()(str(event["code"]))
            concept_col = _concept_column(code.referenced_columns)
            if concept_col is None:
                continue
            needed = (code.referenced_columns | {concept_col}) - columns
            if needed:
                logger.warning(
                    f"Not linking {table}/{event_name} codes to concepts: pre-MEDS table lacks "
                    f"{sorted(needed)} (re-run pre-MEDS with do_overwrite=True)."
                )
                continue
            table_name = event.get("table_name")
            links.append(
                df.select(
                    code=code.polars_expr,
                    table_name=(
                        Parser.expr_to_polars(str(table_name))
                        if table_name is not None
                        else pl.lit(None, dtype=pl.String)
                    ).cast(pl.String),
                    concept_id=pl.col(concept_col).cast(pl.Int64),
                )
                .drop_nulls(["code", "concept_id"])
                .unique()
            )
    schema = {"code": pl.String, "table_name": pl.String, "concept_id": pl.Int64}
    if not links:
        return pl.DataFrame(schema=schema)
    links = (
        pl.concat(links)
        .group_by("code", "table_name")
        .agg(pl.col("concept_id").min(), pl.col("concept_id").n_unique().alias("n"))
        .collect(engine="streaming")
    )
    n_ambiguous = links.filter(pl.col("n") > 1).height
    if n_ambiguous:
        logger.warning(
            f"{n_ambiguous} codes stand for more than one concept (same vocabulary_id and concept_code); "
            "they are linked to the lowest concept_id."
        )
    return links.select(list(schema))


def _concept_column(code_columns: set[str]) -> str | None:
    """The column holding the concept_id behind a code built from `code_columns`, if any."""
    if "preferred_concept_name" in code_columns:
        return "preferred_concept_id"
    concept_ids = [c for c in code_columns if c.endswith("_concept_id")]
    return concept_ids[0] if len(concept_ids) == 1 else None


def _concept_metadata(codes_source: Path) -> pl.DataFrame:
    """One row per concept_id from the pre-MEDS `codes.parquet` (older runs wrote one row per "Maps to")."""
    df = pl.scan_parquet(codes_source)
    if "concept_code" not in df.collect_schema().names():
        df = df.with_columns(pl.lit(None, dtype=pl.String).alias("concept_code"))
    return (
        df.explode("parent_codes")
        .group_by("concept_id")
        .agg(
            pl.col("vocabulary_id").first(),
            pl.col("concept_code").first(),
            pl.col("description").first(),
            pl.col("parent_codes").drop_nulls().unique().sort(),
        )
        .collect()
    )


def _drop_self_parent(parent_codes: pl.Expr) -> pl.Expr:
    """A standard concept "Maps to" itself; as a parent of its own `vocabulary_id//concept_code` code that
    is a self-reference, so it is dropped there. Empty lists become null."""
    parents = parent_codes.list.set_difference(pl.concat_list(pl.col("base_code")))
    return (
        pl.when(parents.list.len() > 0)
        .then(parents.list.sort())
        .otherwise(None)
        .alias("parent_codes")
    )


def write_codes_metadata(codes: pl.DataFrame, codes_fp: Path) -> None:
    """Atomically writes code metadata as valid `meds.CodeMetadataSchema` (`string`, `list<string>`)."""
    table = codes.to_arrow()
    table = table.cast(
        pa.schema(
            [
                pa.field(f.name, ARROW_STRING_TYPES.get(f.type, f.type))
                for f in table.schema
            ]
        )
    )
    CodeMetadataSchema.validate(table)
    _ensure_parent_dir(codes_fp)
    tmp_fp = codes_fp.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp_fp)
    os.replace(tmp_fp, codes_fp)


def _ensure_parent_dir(path: Path) -> None:
    """Ensure parent directory exists for a file path."""
    path.parent.mkdir(parents=True, exist_ok=True)


def enrich_codes_metadata_with_athena(
    codes_fp: Path, pre_MEDS_dir: Path, athena_csv_dir: Path
) -> None:
    """Fills gaps in a finished `codes.parquet` from a local OHDSI Athena vocabulary download.

    A fallback for concepts the source database's `concept` / `concept_relationship` subset lacks; nothing
    the ETL already wrote is changed. Only codes linked to a concept are considered:

    * a null description gets the Athena `concept_name` of the code's concept, through suMEDS
      (`sumeds.enrich_metadata`, valid concepts only). suMEDS matches `VOCABULARY//CODE`, while OMOP codes
      often carry a `concept_id`, so it is given each concept's Athena `vocabulary_id//concept_code`
      rather than the code itself;
    * null parent codes get the `vocabulary_id//concept_code` of the concepts it "Maps to": the source
      relationships, else Athena's, with target codes from the source `concept` table, else Athena's;
    * a null `vocabulary_id` / `concept_code` is taken from Athena.

    Needs the optional `sumeds` dependency (`pip install OMOP_MEDS[athena]`).
    """
    try:
        from sumeds import EnrichmentConfig, enrich_metadata
    except ImportError as e:  # pragma: no cover - depends on the optional extra
        raise ImportError(
            "athena_csv_dir needs suMEDS: pip install 'OMOP_MEDS[athena]'"
        ) from e

    athena_csv_dir = Path(athena_csv_dir)
    codes = pl.from_arrow(pq.read_table(codes_fp))
    todo = codes.filter(
        pl.col("concept_id").is_not_null()
        & (pl.col("description").is_null() | pl.col("parent_codes").is_null())
    )
    ids = todo["concept_id"].unique().implode()

    maps_to = (
        pl.scan_parquet(pre_MEDS_dir / "concept_relationship.parquet")
        .filter(pl.col("relationship_id") == "Maps to")
        .select(
            pl.col("concept_id_1").cast(pl.Int64).alias("concept_id"),
            pl.col("concept_id_2").cast(pl.Int64).alias("target_id"),
        )
        .filter(pl.col("concept_id").is_in(ids))
        .collect()
    )
    without_source_maps = todo.filter(
        ~pl.col("concept_id").is_in(maps_to["concept_id"].implode())
    )
    athena_maps_to = (
        _scan_athena_csv(
            athena_csv_dir / "CONCEPT_RELATIONSHIP.csv",
            ["concept_id_1", "concept_id_2"],
        )
        .filter(
            (pl.col("relationship_id") == "Maps to")
            & pl.col("invalid_reason").is_null()
            & pl.col("concept_id_1").is_in(
                without_source_maps["concept_id"].unique().implode()
            )
        )
        .select(
            pl.col("concept_id_1").alias("concept_id"),
            pl.col("concept_id_2").alias("target_id"),
        )
        .collect(engine="streaming")
    )
    maps_to = pl.concat([maps_to, athena_maps_to])

    key_ids = pl.concat([todo["concept_id"], maps_to["target_id"]]).unique().implode()
    source_keys = (
        pl.scan_parquet(pre_MEDS_dir / "concept.parquet")
        .select(pl.col("concept_id").cast(pl.Int64), "vocabulary_id", "concept_code")
        .filter(pl.col("concept_id").is_in(key_ids))
        .collect()
    )
    athena_keys = (
        _scan_athena_csv(athena_csv_dir / "CONCEPT.csv", ["concept_id"])
        .select("concept_id", "vocabulary_id", "concept_code")
        .filter(
            pl.col("concept_id").is_in(key_ids)
            & ~pl.col("concept_id").is_in(source_keys["concept_id"].implode())
        )
        .collect(engine="streaming")
    )
    keys = pl.concat([source_keys, athena_keys]).with_columns(
        key=pl.concat_str("vocabulary_id", "concept_code", separator="//")
    )

    described = (
        enrich_metadata(
            keys.select(code=pl.col("key")).drop_nulls().unique().lazy(),
            EnrichmentConfig(csv_dir=athena_csv_dir, parent_codes=False),
        )
        .select(key=pl.col("code"), _description=pl.col("description"))
        .collect()
    )
    concept_keys = keys.join(described, on="key", how="left").select(
        "concept_id",
        "_description",
        _vocabulary_id=pl.col("vocabulary_id"),
        _concept_code=pl.col("concept_code"),
    )
    parents = (
        maps_to.join(keys.select(target_id="concept_id", key="key"), on="target_id")
        .group_by("concept_id")
        .agg(_parent_codes=pl.col("key").drop_nulls().unique())
    )
    filled = (
        codes.join(concept_keys, on="concept_id", how="left")
        .join(parents, on="concept_id", how="left")
        .with_columns(
            pl.coalesce(column, f"_{column}").alias(column)
            for column in (
                "description",
                "vocabulary_id",
                "concept_code",
                "parent_codes",
            )
        )
        .with_columns(_drop_self_parent(pl.col("parent_codes")))
        .select(codes.columns)
    )
    write_codes_metadata(filled, codes_fp)
    logger.info(
        "Athena fallback filled "
        f"{filled['description'].is_not_null().sum() - codes['description'].is_not_null().sum()} "
        "descriptions and "
        f"{filled['parent_codes'].is_not_null().sum() - codes['parent_codes'].is_not_null().sum()} "
        f"parent code lists of {codes.height} codes."
    )


def _scan_athena_csv(fp: Path, id_columns: list[str]) -> pl.LazyFrame:
    """Scans an Athena vocabulary file (tab-separated, unquoted) with its concept id columns as Int64."""
    if not fp.is_file():
        raise FileNotFoundError(f"Athena vocabulary file not found: {fp}")
    return pl.scan_csv(
        fp,
        separator="\t",
        quote_char=None,
        null_values=[""],
        infer_schema=False,
    ).with_columns(pl.col(id_columns).cast(pl.Int64))
