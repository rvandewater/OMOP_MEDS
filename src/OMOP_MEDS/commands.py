import logging
import os
import subprocess

from omegaconf import DictConfig

logger = logging.getLogger(__name__)


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
