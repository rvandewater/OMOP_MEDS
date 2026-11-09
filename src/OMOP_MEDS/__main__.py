#!/usr/bin/env python
"""Deprecated `OMOP_MEDS` / `MEDS_extract-OMOP` commands: they run `meds-extract-run spec=OMOP`.

The ETL is the MEDS-Extract spec registered as `OMOP`, run with `meds-extract-run` (see the README). These two
commands only translate the arguments of the old runner for one more release, and will be removed:

    OMOP_MEDS raw_input_dir=RAW root_output_dir=ROOT [++prefer_source=True ...]

runs

    meds-extract-run spec=OMOP output_dir=ROOT/MEDS_cohort do_download=false input_dir=RAW \
        "overrides=['++stage_cfg.prefer_source=True', ...]"

The MEDS cohort stays in `ROOT/MEDS_cohort`; the pre-MEDS tables move to `ROOT/MEDS_cohort/omop_pre_meds`.
"""

import os
import shutil
import sys
import sysconfig
from pathlib import Path

from .stages import CODES_METADATA_DEFAULTS, PRE_MEDS_DEFAULTS

#: Old runner options that are now options of the OMOP stages, passed as `++stage_cfg.<option>` overrides.
STAGE_OPTIONS = frozenset({*PRE_MEDS_DEFAULTS, *CODES_METADATA_DEFAULTS})
#: `meds-extract-run` options, passed on unchanged.
RUN_OPTIONS = frozenset(
    {
        "dataset_version",
        "do_cleanup_intermediates",
        "skip_stages",
        "stage_runner_fp",
        "do_profile",
        "overrides",
    }
)


def _bool(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes"}


def translate(args: list[str], env: dict[str, str] | None = None) -> list[str]:
    """The `meds-extract-run` arguments for the old runner's arguments `args`.

    `DATASET_NAME` in `env` names the build, as it did for the old runner.

    Examples:
        >>> translate(["raw_input_dir=/raw", "root_output_dir=/root", "++limit_subjects=50"], env={})
        ['spec=OMOP', 'output_dir=/root/MEDS_cohort', 'do_download=false', 'input_dir=/raw',
         "overrides=['++stage_cfg.limit_subjects=50']"]
        >>> translate(["root_output_dir=/root", "do_download=True", "do_demo=True", "do_enrich=False"],
        ...           env={"DATASET_NAME": "DEMO"})
        ['spec=OMOP', 'output_dir=/root/MEDS_cohort', 'dataset_key=demo', 'download_dest_dir=/root/raw_input',
         'skip_stages=[sumeds_enrich]', "overrides=['etl_metadata.dataset_name=DEMO']"]
        >>> translate(["root_output_dir=/root", "sumeds_args=[--strict]"], env={})
        Traceback (most recent call last):
            ...
        ValueError: OMOP_MEDS no longer takes sumeds_args=; run meds-extract-run spec=OMOP (see the README).
    """
    env = os.environ if env is None else env
    given = {}
    for arg in args:
        key, sep, value = arg.lstrip("+").partition("=")
        if not sep:
            raise ValueError(f"Expected key=value arguments, got {arg!r}.")
        given[key] = value
    if "root_output_dir" not in given:
        raise ValueError("root_output_dir= is required.")
    root = Path(given.pop("root_output_dir"))
    raw = given.pop("raw_input_dir", str(root / "raw_input"))
    argv = ["spec=OMOP", f"output_dir={root / 'MEDS_cohort'}"]
    if _bool(given.pop("do_download", "false")):
        # The old runner downloaded the MIMIC-IV OMOP demo with and without do_demo.
        given.pop("do_demo", None)
        argv += ["dataset_key=demo", f"download_dest_dir={raw}"]
    else:
        given.pop("do_demo", None)
        argv += ["do_download=false", f"input_dir={raw}"]
    if not _bool(given.pop("do_enrich", "true")):
        argv.append("skip_stages=[sumeds_enrich]")
    given.pop("do_overwrite", None)  # handled by main()
    overrides = []
    if env.get("DATASET_NAME"):
        overrides.append(f"etl_metadata.dataset_name={env['DATASET_NAME']}")
    for key, value in given.items():
        if key in STAGE_OPTIONS:
            overrides.append(f"++stage_cfg.{key}={value}")
        elif key in RUN_OPTIONS:
            argv.append(f"{key}={value}")
        else:
            raise ValueError(
                f"OMOP_MEDS no longer takes {key}=; run meds-extract-run spec=OMOP (see the README)."
            )
    if overrides:
        argv.append("overrides=[" + ",".join(f"'{o}'" for o in overrides) + "]")
    return argv


def main() -> None:
    """Run `meds-extract-run spec=OMOP` with the old runner's arguments, after a deprecation warning."""
    from MEDS_extract.run.cli import main as meds_extract_run

    args = sys.argv[1:]
    try:
        argv = translate(args)
    except ValueError as e:
        sys.exit(f"{Path(sys.argv[0]).name}: {e}")
    print(
        f"{Path(sys.argv[0]).name} is deprecated and will be removed; it runs:\n"
        f"  meds-extract-run {' '.join(argv)}",
        file=sys.stderr,
    )
    given = dict(a.lstrip("+").partition("=")[::2] for a in args)
    if _bool(given.get("do_overwrite", "false")):
        # As the old runner did, start from an empty output.
        shutil.rmtree(
            Path(given["root_output_dir"]) / "MEDS_cohort", ignore_errors=True
        )
    # MEDS-Transforms starts every stage as `MEDS_transform-stage` from PATH: use this environment's.
    scripts = sysconfig.get_path("scripts")
    os.environ["PATH"] = os.pathsep.join([scripts, os.environ.get("PATH", "")])
    sys.argv = ["meds-extract-run", *argv]
    meds_extract_run()


if __name__ == "__main__":
    main()
