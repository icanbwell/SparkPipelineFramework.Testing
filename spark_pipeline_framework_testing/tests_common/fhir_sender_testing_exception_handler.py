import json
import os
from pathlib import Path
from typing import Optional, Callable, Union, Any, Dict, List

from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException
from mockserver_client.exceptions.mock_server_json_content_mismatch_exception import (
    MockServerJsonContentMismatchException,
)
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)


def handle_fhir_sender_exception(
    e: FriendlySparkException,
    temp_folder: Path,
    func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]],
) -> None:
    if not isinstance(e.exception, FhirSenderException):
        raise

    fhir_sender_exception: FhirSenderException = e.exception
    if not isinstance(
        fhir_sender_exception.exception, MockServerJsonContentMismatchException
    ):
        raise

    json_content_mismatch_exception: MockServerJsonContentMismatchException = (
        fhir_sender_exception.exception
    )
    expected_path = json_content_mismatch_exception.expected_file_path
    expected_file_name: str = os.path.basename(expected_path)
    # create a temp file to launch the diff tool
    # use .command: https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
    compare_sh_path = Path(temp_folder).joinpath(
        f"compare_{expected_file_name}.command"
    )
    # write actual to result_path
    result_path: Path = temp_folder.joinpath(expected_file_name)
    if json_content_mismatch_exception.actual_json is not None:
        with open(result_path, "w") as file_result:
            actual_json: List[Dict[str, Any]] | Dict[str, Any] = (
                json_content_mismatch_exception.actual_json
            )
            if isinstance(actual_json, list) and len(actual_json) == 1:
                actual_json = actual_json[0]
            file_result.write(json.dumps(actual_json, indent=2))
    with open(compare_sh_path, "w") as compare_sh:
        compare_sh.write(
            'open -na "PyCharm.app" --args diff '
            f"{func_path_modifier(result_path) if func_path_modifier else result_path}"
            f" {func_path_modifier(expected_path) if func_path_modifier else expected_path}"
        )
        os.fchmod(compare_sh.fileno(), 0o7777)
        # pytest.fail(
        #     str(
        #         func_path_modifier(compare_sh_path)
        #         if func_path_modifier
        #         else compare_sh_path
        #     )
        # )
