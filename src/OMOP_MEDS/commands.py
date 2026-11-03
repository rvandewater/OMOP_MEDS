import logging
import os
import shutil
import subprocess
import sysconfig
from pathlib import Path

from omegaconf import DictConfig

logger = logging.getLogger(__name__)

MEDS_PIPELINE_CMD = "MEDS_transform-pipeline"


def path_with_scripts_dir_first(
    command: str = MEDS_PIPELINE_CMD,
    path: str | None = None,
    scripts_dir: Path | str | None = None,
) -> str:
    """Return a `PATH` that resolves `command` from this interpreter's scripts directory first.

    The ETL shells out to `MEDS_transform-pipeline`, which in turn launches every stage as
    `MEDS_transform-stage` through a shell. Resolved from `PATH` alone, both can come from a different
    environment (another virtualenv, a global install) whose MEDS-Extract version does not understand the
    pipeline and MESSY configs this package ships. When the interpreter running this package has `command`
    installed next to it, that directory is moved to the front of `PATH`; otherwise `PATH` is returned
    unchanged so installs that put console scripts elsewhere keep working.

    Args:
        command: The console script that must be found in `scripts_dir` for it to be prepended.
        path: The `PATH` to extend; defaults to the current `PATH`.
        scripts_dir: The directory to prefer; defaults to this interpreter's scripts directory.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     tool = Path(d) / "tool"
        ...     _ = tool.write_text("#!/bin/sh\\n")
        ...     tool.chmod(0o755)
        ...     path = os.pathsep.join(["/usr/bin", d, "/bin"])
        ...     new = path_with_scripts_dir_first("tool", path, scripts_dir=d).split(os.pathsep)
        ...     new == [d, "/usr/bin", "/bin"]
        True

        A directory without the command, or one already first on `PATH`, leaves `PATH` as it was:

        >>> with tempfile.TemporaryDirectory() as d:
        ...     path_with_scripts_dir_first("tool", "/usr/bin", scripts_dir=d)
        '/usr/bin'
        >>> with tempfile.TemporaryDirectory() as d:
        ...     tool = Path(d) / "tool"
        ...     _ = tool.write_text("#!/bin/sh\\n")
        ...     tool.chmod(0o755)
        ...     path_with_scripts_dir_first("tool", d, scripts_dir=d) == d
        True
    """
    path = os.environ.get("PATH", "") if path is None else path
    scripts_dir = str(
        sysconfig.get_path("scripts") if scripts_dir is None else scripts_dir
    )
    if shutil.which(command, path=scripts_dir) is None:
        return path

    entries = [p for p in path.split(os.pathsep) if p]
    rest = [p for p in entries if os.path.normcase(p) != os.path.normcase(scripts_dir)]
    return os.pathsep.join([scripts_dir, *rest])


def run_command(
    command_parts: list[str],
    cfg: DictConfig | dict | None = None,
    runner_fn: callable = subprocess.run,
    env: dict[str, str] | None = None,
):
    """Run a command without shell interpolation."""
    if cfg is not None:
        do_overwrite = cfg.get("do_overwrite", None)
        do_profile = cfg.get("do_profile", False)
        seed = cfg.get("seed", None)
        if do_overwrite is not None and not any(
            x.startswith("do_overwrite=") for x in command_parts
        ):
            command_parts.append(f"do_overwrite={do_overwrite}")
        if seed is not None and not any(x.startswith("seed=") for x in command_parts):
            command_parts.append(f"seed={seed}")
        if do_profile:
            command_parts.append("--do_profile")

    logger.info("Running command: %s", command_parts)
    command_out = runner_fn(
        command_parts, capture_output=True, env={**os.environ, **(env or {})}
    )

    stderr = command_out.stderr.decode()
    stdout = command_out.stdout.decode()

    logger.info("Command stdout:\n%s", stdout)
    if stderr:
        logger.warning("Command stderr:\n%s", stderr)

    if command_out.returncode != 0:
        raise ValueError(
            "Command failed with return code "
            f"{command_out.returncode}.\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
