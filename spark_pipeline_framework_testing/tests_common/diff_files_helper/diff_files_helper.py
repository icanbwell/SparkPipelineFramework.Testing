import json
import os
from pathlib import Path
from typing import Any

from spark_pipeline_framework_testing.tests_common.path_converter import (
    convert_path_from_docker,
)


class DiffFilesHelper:
    @staticmethod
    def create_diff_command(
        expected_path: Path, temp_folder: Path, actual: Any
    ) -> Path:
        expected_file_name: str = os.path.basename(expected_path)
        # create a temp file to launch the diff tool
        # use .command:
        # https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
        compare_sh_path = Path(temp_folder).joinpath(
            f"compare_es_{expected_file_name}.command"
        )
        # write actual to result_path
        os.makedirs(temp_folder.joinpath("actual_es_calls"), exist_ok=True)
        result_path: Path = temp_folder.joinpath("actual_es_calls").joinpath(
            expected_file_name
        )
        with open(result_path, "w") as file_result:
            file_result.write(json.dumps(actual, indent=2))
        with open(compare_sh_path, "w") as compare_sh:
            compare_sh.write(
                f"/usr/local/bin/charm diff "
                f"{convert_path_from_docker(result_path) if convert_path_from_docker else result_path} "
                f"{convert_path_from_docker(expected_path) if convert_path_from_docker else expected_path}"
            )
            os.fchmod(compare_sh.fileno(), 0o7777)

        return compare_sh_path
