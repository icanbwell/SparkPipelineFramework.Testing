import json
import os
from pathlib import Path
from typing import Optional, Callable, Union

from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException  # type: ignore
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)

from spark_pipeline_framework_testing.mockserver_client.exceptions.mock_server_json_content_mismatch_exception import (
    MockServerJsonContentMismatchException,
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
    with open(result_path, "w") as file_result:
        file_result.write(json.dumps(json_content_mismatch_exception.actual, indent=2))
    with open(compare_sh_path, "w") as compare_sh:
        compare_sh.write(
            f"/usr/local/bin/charm diff "
            f"{func_path_modifier(result_path) if func_path_modifier else result_path} "
            f"{func_path_modifier(expected_path) if func_path_modifier else expected_path}"
        )
        os.fchmod(compare_sh.fileno(), 0o7777)
        # pytest.fail(
        #     str(
        #         func_path_modifier(compare_sh_path)
        #         if func_path_modifier
        #         else compare_sh_path
        #     )
        # )
