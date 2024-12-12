import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory


def test_e2e():
    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        do_overwrite = True
        do_demo = True
        do_download = True

        command_parts = [
            "MEDS_extract-sample_dataset",
            f"root_output_dir={str(root.resolve())}",
            f"do_download={do_download}",
            f"do_overwrite={do_overwrite}",
            f"do_demo={do_demo}",
        ]

        full_cmd = " ".join(command_parts)
        command_out = subprocess.run(full_cmd, shell=True, capture_output=True)

        stderr = command_out.stderr.decode()
        stdout = command_out.stdout.decode()

        if command_out.returncode != 0:
            print(f"Command failed with return code {command_out.returncode}.")
            print(f"Command stdout:\n{stdout}")
            print(f"Command stderr:\n{stderr}")
            raise ValueError(f"Command failed with return code {command_out.returncode}.")
