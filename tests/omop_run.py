"""Run the ETL as users do: `meds-extract-run spec=OMOP ...`, from this test environment."""

import os
import subprocess
import sysconfig
from pathlib import Path

SCRIPTS_DIR = Path(sysconfig.get_path("scripts"))


def run_omop(
    output_dir: Path,
    input_dir: Path | None,
    *args: str,
    env: dict[str, str] | None = None,
    check: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess:
    """`meds-extract-run spec=OMOP output_dir=OUTPUT_DIR do_download=false input_dir=INPUT_DIR ARGS...`.

    Without `input_dir`, the arguments must say where the data comes from (e.g. `dataset_key=demo`). Fails with
    the command's output unless `check=False`.
    """
    command = [
        str(SCRIPTS_DIR / "meds-extract-run"),
        "spec=OMOP",
        f"output_dir={output_dir}",
    ]
    if input_dir is not None:
        command += ["do_download=false", f"input_dir={input_dir}"]
    command += list(args)
    # MEDS-Transforms starts every stage as `MEDS_transform-stage` from PATH.
    full_env = {
        **os.environ,
        "PATH": os.pathsep.join([str(SCRIPTS_DIR), os.environ.get("PATH", "")]),
        "HYDRA_FULL_ERROR": "1",
        **(env or {}),
    }
    result = subprocess.run(
        command, capture_output=True, text=True, env=full_env, timeout=timeout
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"{' '.join(command)} failed with exit code {result.returncode}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}\n"
            f"pipeline log:\n{_pipeline_log(output_dir)}"
        )
    return result


def _pipeline_log(output_dir: Path) -> str:
    log = Path(output_dir) / ".logs" / "pipeline.log"
    return log.read_text()[-20_000:] if log.is_file() else "(none)"
