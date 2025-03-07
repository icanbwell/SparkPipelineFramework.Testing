from glob import glob
import json
import os
from abc import ABC, abstractmethod
from logging import Logger
from os import listdir
from os.path import isfile, join
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, Union, Callable, TYPE_CHECKING

import dictdiffer

import pytest
from deprecated import deprecated
from mockserver_client.exceptions.mock_server_expectation_not_found_exception import (
    MockServerExpectationNotFoundException,
)
from mockserver_client.exceptions.mock_server_json_content_mismatch_exception import (
    MockServerJsonContentMismatchException,
)
from mockserver_client.exceptions.mock_server_request_not_found_exception import (
    MockServerRequestNotFoundException,
)
from mockserver_client.mock_expectation import MockExpectation
from mockserver_client.mock_request import MockRequest
from mockserver_client.mockserver_client import MockServerFriendlyClient
from mockserver_client.mockserver_verify_exception import MockServerVerifyException
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, DataType
from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
)
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_list_catalog_table_names,
)

from spark_pipeline_framework_testing.tests_common.path_converter import (
    convert_path_from_docker,
)

if TYPE_CHECKING:
    from spark_pipeline_framework_testing.test_classes.input_types import (
        FhirCalls,
        FileInput,
        MockFhirRequest,
    )
from spark_pipeline_framework_testing.testing_exception import (
    SparkPipelineFrameworkTestingException,
)
from spark_pipeline_framework_testing.tests_common.common_functions import (
    write_schema_to_output,
    get_view_name_from_file_path,
    get_file_extension_from_file_path,
    camel_case_to_snake_case,
)


class Validator(ABC):
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        pass


@deprecated("This function is deprecated. Use MockRequestValidator instead")
class MockCallValidator(Validator):
    """
    validates Mock calls
    """

    def __init__(
        self,
        related_inputs: Optional[
            Union[
                List["FhirCalls"],
                "FhirCalls",
                List["MockFhirRequest"],
                "MockFhirRequest",
            ]
        ],
        fail_on_warning: bool = False,
    ) -> None:
        self.fail_on_warning: bool = fail_on_warning
        if related_inputs:
            self.related_inputs = (
                related_inputs if isinstance(related_inputs, list) else [related_inputs]
            )

    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        assert mock_client
        # why cant we just provide the path to the validator init if it just needs the path to the directory?
        data_folder_path: Path = test_path.joinpath(
            self.related_inputs[0].fhir_calls_folder
        )
        try:
            mock_client.verify_expectations(
                test_name=test_name, files=self.get_input_files()
            )
        except MockServerVerifyException as e:
            compare_files: List[str] = []
            existing_resource_folders: List[str] = [
                f.name for f in list(os.scandir(data_folder_path)) if f.is_dir()
            ]
            for exception in e.exceptions:
                if isinstance(exception, MockServerJsonContentMismatchException):
                    expected_path = exception.expected_file_path
                    assert expected_path
                    if len(expected_path.parts) > 0:
                        expected_file_name: str = os.path.basename(expected_path)
                        # create a temp file to launch the diff tool
                        # use .command:
                        # https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
                        compare_sh_path = temp_folder_path.joinpath(
                            f"compare_http_{expected_file_name}.command"
                        )
                        # write actual to result_path
                        os.makedirs(
                            temp_folder_path.joinpath("actual_http_calls"),
                            exist_ok=True,
                        )
                        result_path: Path = temp_folder_path.joinpath(
                            "actual_http_calls"
                        ).joinpath(expected_file_name)
                        if exception.actual_json is not None:
                            with open(result_path, "w") as file_result:
                                actual_json: List[Dict[str, Any]] | Dict[str, Any] = (
                                    exception.actual_json
                                )
                                if (
                                    isinstance(actual_json, list)
                                    and len(actual_json) == 1
                                ):
                                    actual_json = actual_json[0]
                                file_result.write(json.dumps(actual_json, indent=2))
                        with open(compare_sh_path, "w") as compare_sh:
                            compare_sh.write(
                                'open -na "PyCharm.app" --args diff '
                                f"{convert_path_from_docker(result_path)}"
                                f" {convert_path_from_docker(expected_path)}"
                            )
                            os.fchmod(compare_sh.fileno(), 0o7777)
                        compare_files.append(str(compare_sh_path))
                elif isinstance(exception, MockServerRequestNotFoundException):
                    # write to file
                    resource_obj: Optional[Dict[str, Any]] = (
                        (
                            exception.json_dict[0]
                            if len(exception.json_dict) > 0
                            else None
                        )
                        if isinstance(exception.json_dict, list)
                        else exception.json_dict
                    )
                    if resource_obj:
                        # assert "resourceType" in resource_obj, resource_obj
                        resource_type: Optional[str] = resource_obj.get(
                            "resourceType", None
                        )
                        if resource_type:
                            assert "id" in resource_obj, resource_obj
                            resource_id = resource_obj["id"]
                            resource_type_folder_name: str = camel_case_to_snake_case(
                                resource_type
                            )
                            resource_path: Path = data_folder_path.joinpath(
                                f"{resource_type_folder_name}"
                            )
                            # if folder does not exist or is empty then write out the files
                            if (
                                resource_type_folder_name
                                not in existing_resource_folders
                            ):
                                os.makedirs(resource_path, exist_ok=True)
                                resource_file_path: Path = resource_path.joinpath(
                                    f"{resource_id}.json"
                                )
                                # check if file already exists
                                if os.path.exists(resource_file_path):
                                    # # see if the content matches
                                    # with open(resource_file_path, "r") as file:
                                    #     file_json: Dict[str, Any] = json.loads(file.read())
                                    # check if file with .1, .2 etc. exists
                                    for index in range(1, 10):
                                        resource_file_path = resource_path.joinpath(
                                            f"{resource_id}.{index}.json"
                                        )
                                        if not os.path.exists(resource_file_path):
                                            break
                                # write the output to disk
                                with open(resource_file_path, "w") as file:
                                    file.write(json.dumps(resource_obj, indent=2))
                                logger.info(
                                    f"Writing http calls file to : {resource_file_path}"
                                )
                        else:
                            logger.info(
                                f"a non standard fhir request was expected and not found. {resource_obj}"
                            )
                elif isinstance(exception, MockServerExpectationNotFoundException):
                    # add to error
                    pass
                else:
                    raise Exception(f"unknown exception type: {type(exception)}")
            for c in compare_files:
                logger.info(c)
            compare_files_text: str = "\n".join(
                [str(convert_path_from_docker(c)) for c in compare_files]
            )
            content_not_matched_exceptions: List[
                MockServerJsonContentMismatchException
            ] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerJsonContentMismatchException)
            ]
            failure_message: str = ""
            if len(content_not_matched_exceptions) > 0:
                if compare_files_text:
                    failure_message += (
                        f"{len(content_not_matched_exceptions)} files did not match: \n"
                    )
                    failure_message += f"{compare_files_text}\n"
                    for content_not_matched_exception in content_not_matched_exceptions:
                        failure_message += f"-----------  {content_not_matched_exception.url} File: {content_not_matched_exception.expected_file_path} -----------\n"
                        for difference in content_not_matched_exception.differences:
                            failure_message += f"{difference}\n"

                else:
                    failure_message += f"{len(content_not_matched_exceptions)} requests did not match: \n"
                    msg = "\n".join([str(c) for c in content_not_matched_exceptions])
                    failure_message += f"{msg}\n"
                    for content_not_matched_exception in content_not_matched_exceptions:
                        failure_message += f"-----------  {content_not_matched_exception.url} File:  {content_not_matched_exception.expected_file_path} -----------\n"
                        for difference in content_not_matched_exception.differences:
                            failure_message += f"{difference}\n"

            expectations_not_met_exceptions: List[
                MockServerExpectationNotFoundException
            ] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerExpectationNotFoundException)
            ]
            if len(expectations_not_met_exceptions) > 0:
                failure_message += "\nThese requests were expected but not made:\n-----------------------\n"
                failure_message += "\n".join(
                    [
                        "\n-----------------------\n"
                        + str(c)
                        + "\n-----------------------\n"
                        for c in expectations_not_met_exceptions
                    ]
                )

            unexpected_requests: List[MockServerRequestNotFoundException] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerRequestNotFoundException)
            ]
            warning_message = ""
            if len(unexpected_requests) > 0:
                warning_message += "\nUnexpected requests:\n"
                warning_message += "\n".join(
                    [f"{c.url}: {c.json_dict}" for c in unexpected_requests]
                )
                logger.info(warning_message)

            # now try to match up unexpected requests with unmatched expectations
            for unexpected_request in unexpected_requests:
                for expectations_not_met_exception in expectations_not_met_exceptions:
                    # check if the url matches.  If so then the content is different
                    if unexpected_request.request.matches_without_body(
                        expectations_not_met_exception.expectation
                    ):
                        comparison_result = list(
                            dictdiffer.diff(
                                expectations_not_met_exception.json_list,
                                unexpected_request.json_dict,
                            )
                        )
                        comparison_result_text = str(comparison_result)

                        warning_message += (
                            "Content of request is different than expected for url: "
                            f"{unexpected_request.url}"
                            + f"\nExpected:{expectations_not_met_exception}"
                            + f"\nActual:{unexpected_request}"
                            + f"\nDifferences: \n{comparison_result_text}\n"
                        )

            # if there is a failure then stop the test
            if failure_message or (warning_message and self.fail_on_warning):
                # want to print the warnings out here too to make it easier to see in test output
                test_failure_message = "\nMOCKED REQUEST MATCH FAILURE:\n"
                test_failure_message += warning_message
                test_failure_message += "\n" + failure_message
                all_requests: List[MockRequest] = mock_client.retrieve_requests()
                all_expectations: List[MockExpectation] = mock_client.expectations

                all_expectations_str = "\n".join(
                    [f"{expectation.request}" for expectation in all_expectations]
                )
                logger.info(
                    f"\n ---- ALL EXPECTATIONS -------\n{all_expectations_str}\n--- END EXPECTATIONS ---\n"
                )

                all_requests_str = "\n".join([f"{request}" for request in all_requests])
                logger.info(
                    f"\n ---- ALL REQUESTS -------\n{all_requests_str}\n--- END REQUESTS ---\n"
                )

                pytest.fail(test_failure_message)
            elif warning_message:
                print(warning_message)

    def get_input_files(self) -> List[str]:
        if not self.related_inputs:
            return []
        all_files: List[str] = []
        related_input: Union["FhirCalls", "MockFhirRequest"]
        # why use related_inputs when we just need the path to the directories?
        for related_input in self.related_inputs:
            if related_input:
                all_files.extend(related_input.mocked_files)  # type: ignore
        return all_files


class MockRequestValidator(Validator):
    def __init__(
        self,
        mock_requests_folder: Optional[str] = None,
        fail_on_warning: bool = False,
    ) -> None:
        """
        Validates all mocked requests on the mock server. Only one of these is needed per test it will validate all
        expectations on the mock server.

        :param mock_requests_folder: name of the folder where the mock request files are. if set the files
                                        in the folder will have their contents compared to the actual request and
                                        a diff will be generated
        :param fail_on_warning: if true the test will fail if there are warnings, an example is a request for
                                which there is no expectation
        """
        self.fail_on_warning: bool = fail_on_warning
        self.mock_requests_folder = mock_requests_folder

    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        assert mock_client
        data_folder_path: Optional[Path] = None
        if self.mock_requests_folder:
            data_folder_path = test_path.joinpath(self.mock_requests_folder)
        try:
            mock_client.verify_expectations(
                test_name=test_name, files=self.get_input_files(data_folder_path)
            )
        except MockServerVerifyException as e:
            compare_files: List[str] = []
            existing_resource_folders: List[str] = [
                f.name for f in list(os.scandir(data_folder_path)) if f.is_dir()
            ]
            for exception in e.exceptions:
                if isinstance(exception, MockServerJsonContentMismatchException):
                    expected_path = exception.expected_file_path
                    assert expected_path
                    if len(expected_path.parts) > 0:
                        expected_file_name: str = os.path.basename(expected_path)
                        # create a temp file to launch the diff tool
                        # use .command:
                        # https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
                        compare_sh_path = temp_folder_path.joinpath(
                            f"compare_http_{expected_file_name}.command"
                        )
                        # write actual to result_path
                        os.makedirs(
                            temp_folder_path.joinpath("actual_http_calls"),
                            exist_ok=True,
                        )
                        result_path: Path = temp_folder_path.joinpath(
                            "actual_http_calls"
                        ).joinpath(expected_file_name)
                        if exception.actual_json is not None:
                            with open(result_path, "w") as file_result:
                                actual_json: List[Dict[str, Any]] | Dict[str, Any] = (
                                    exception.actual_json
                                )
                                if (
                                    isinstance(actual_json, list)
                                    and len(actual_json) == 1
                                ):
                                    actual_json = actual_json[0]
                                file_result.write(json.dumps(actual_json, indent=2))
                        with open(compare_sh_path, "w") as compare_sh:
                            compare_sh.write(
                                'open -na "PyCharm.app" --args diff '
                                f"{convert_path_from_docker(result_path)}"
                                f" {convert_path_from_docker(expected_path)}"
                            )
                            os.fchmod(compare_sh.fileno(), 0o7777)
                        compare_files.append(str(compare_sh_path))
                elif isinstance(exception, MockServerRequestNotFoundException):
                    # attempts to write the body of the request (exception.json_dict) to a file with id as the name
                    # if the following conditions are met:
                    # 1. the body of the request (exception.json_dict) is a FHIR resource
                    # 2. the data_folder_path is set
                    # 3. there is not an existing subfolder of data_folder_path with the name of the
                    # resource converted to camel case
                    resource_obj: Optional[Dict[str, Any]] = (
                        (
                            exception.json_dict[0]
                            if len(exception.json_dict) > 0
                            else None
                        )
                        if isinstance(exception.json_dict, list)
                        else exception.json_dict
                    )
                    if resource_obj and data_folder_path:
                        # assert "resourceType" in resource_obj, resource_obj
                        resource_type: Optional[str] = resource_obj.get(
                            "resourceType", None
                        )
                        if resource_type:
                            assert "id" in resource_obj, resource_obj
                            resource_id = resource_obj["id"]
                            resource_type_folder_name: str = camel_case_to_snake_case(
                                resource_type
                            )
                            resource_path: Path = data_folder_path.joinpath(
                                f"{resource_type_folder_name}"
                            )
                            # if folder does not exist or is empty then write out the files
                            if (
                                resource_type_folder_name
                                not in existing_resource_folders
                            ):
                                os.makedirs(resource_path, exist_ok=True)
                                resource_file_path: Path = resource_path.joinpath(
                                    f"{resource_id}.json"
                                )
                                # check if file already exists
                                if os.path.exists(resource_file_path):
                                    # # see if the content matches
                                    # with open(resource_file_path, "r") as file:
                                    #     file_json: Dict[str, Any] = json.loads(file.read())
                                    # check if file with .1, .2 etc. exists
                                    for index in range(1, 10):
                                        resource_file_path = resource_path.joinpath(
                                            f"{resource_id}.{index}.json"
                                        )
                                        if not os.path.exists(resource_file_path):
                                            break
                                # write the output to disk
                                with open(resource_file_path, "w") as file:
                                    file.write(json.dumps(resource_obj, indent=2))
                                logger.info(
                                    f"Writing http calls file to : {resource_file_path}"
                                )
                        else:
                            logger.info(
                                f"a non standard fhir request was expected and not found. {resource_obj}"
                            )
                elif isinstance(exception, MockServerExpectationNotFoundException):
                    # add to error
                    pass
                else:
                    raise Exception(f"unknown exception type: {type(exception)}")
            for c in compare_files:
                logger.info(c)
            compare_files_text: str = "\n".join(
                [str(convert_path_from_docker(c)) for c in compare_files]
            )
            content_not_matched_exceptions: List[
                MockServerJsonContentMismatchException
            ] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerJsonContentMismatchException)
            ]
            failure_message: str = ""
            if len(content_not_matched_exceptions) > 0:
                if compare_files_text:
                    failure_message += (
                        f"{len(content_not_matched_exceptions)} files did not match: \n"
                    )
                    failure_message += f"{compare_files_text}\n"
                    for content_not_matched_exception in content_not_matched_exceptions:
                        failure_message += f"-----------  {content_not_matched_exception.url} File {content_not_matched_exception.expected_file_path} -----------\n"
                        for difference in content_not_matched_exception.differences:
                            failure_message += f"{difference}\n"
                else:
                    failure_message += f"{len(content_not_matched_exceptions)} requests did not match: \n\n"
                    msg = "\n".join([str(c) for c in content_not_matched_exceptions])
                    failure_message += f"{msg}\n"
                    for content_not_matched_exception in content_not_matched_exceptions:
                        failure_message += f"----------- {content_not_matched_exception.url} File: {content_not_matched_exception.expected_file_path} -----------\n"
                        for difference in content_not_matched_exception.differences:
                            failure_message += f"{difference}\n"

            expectations_not_met_exceptions: List[
                MockServerExpectationNotFoundException
            ] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerExpectationNotFoundException)
            ]
            if len(expectations_not_met_exceptions) > 0:
                failure_message += "\nThese requests were expected but not made:\n-----------------------\n"
                failure_message += "\n".join(
                    [
                        "\n-----------------------\n"
                        + str(c)
                        + "\n-----------------------\n"
                        for c in expectations_not_met_exceptions
                    ]
                )

            unexpected_requests: List[MockServerRequestNotFoundException] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerRequestNotFoundException)
            ]
            warning_message = ""
            if len(unexpected_requests) > 0:
                warning_message += "\nUnexpected requests:\n"
                warning_message += "\n".join([f"{c}" for c in unexpected_requests])
                warning_message += "\n"
                logger.info(warning_message)

            # now try to match up unexpected requests with unmatched expectations
            for unexpected_request in unexpected_requests:
                for expectations_not_met_exception in expectations_not_met_exceptions:
                    # check if the url matches.  If so then the content is different
                    if unexpected_request.url == expectations_not_met_exception.url:
                        comparison_result = list(
                            dictdiffer.diff(
                                expectations_not_met_exception.json_list,
                                unexpected_request.json_dict,
                            )
                        )
                        comparison_result_text = str(comparison_result)
                        if unexpected_request.json_dict is None:
                            comparison_result_text = "Request has no body"
                        elif expectations_not_met_exception.json_list is None:
                            comparison_result_text = (
                                "Expected no body but request has a body"
                            )
                        warning_message += (
                            "Content of request is different than expected for url: "
                            f"{unexpected_request.url}"
                            + f"\nExpected:{expectations_not_met_exception}"
                            + f"\nActual:{unexpected_request}"
                            + f"\nDifferences: \n{comparison_result_text}\n"
                        )

            # if there is a failure then stop the test
            if failure_message or (warning_message and self.fail_on_warning):
                # want to print the warnings out here too to make it easier to see in test output
                test_failure_message = "\nMOCKED REQUEST FAILURE:\n"
                test_failure_message += failure_message
                test_failure_message += warning_message
                all_requests: List[MockRequest] = mock_client.retrieve_requests()
                all_expectations: List[MockExpectation] = mock_client.expectations
                all_expectations_str = "\n".join(
                    [f"{expectation.request}" for expectation in all_expectations]
                )
                logger.info(
                    f"\n ---- ALL EXPECTATIONS -------\n{all_expectations_str}\n--- END EXPECTATIONS ---\n"
                )

                all_requests_str = "\n".join([f"{request}" for request in all_requests])
                logger.info(
                    f"\n ---- ALL REQUESTS -------\n{all_requests_str}\n--- END REQUESTS ---\n"
                )

                pytest.fail(test_failure_message)
            elif warning_message:
                print(warning_message)

    # noinspection PyMethodMayBeStatic
    def get_input_files(self, data_folder_path: Optional[Path]) -> List[str]:
        if data_folder_path is None:
            return []
        files: List[str] = sorted(
            glob(str(data_folder_path.joinpath("**/*.json")), recursive=True)
        )
        return files


class OutputFileValidator(Validator):
    """
    Validates the output files from a feature or pipeline
    """

    # noinspection PyDefaultArgument
    def __init__(
        self,
        related_inputs: Optional[Union[List["FileInput"], "FileInput"]] = None,
        func_path_modifier: Optional[
            Callable[[Union[Path, str]], Union[Path, str]]
        ] = convert_path_from_docker,
        sort_output_by: Optional[List[str]] = ["id"],
        auto_sort: Optional[bool] = True,
        output_as_json_only: bool = True,
        apply_schema_to_output: bool = True,
        ignore_views_for_output: Optional[List[str]] = None,
        output_folder: str = "output",
        output_schema_folder: str = "output_schema",
        output_schema: Optional[
            Union[StructType, Dict[str, StructType], DataType]
        ] = None,
    ):
        """
        Validates the output files from a feature or pipeline


        :param related_inputs: the corresponding input for this validator, optional if the pipeline input comes
            from calling an api
        :param func_path_modifier: in case you want to change paths e.g. docker to local
        :param sort_output_by: order of column names [col1, col2,...]  (default is ["id"])
        :param output_as_json_only: save output as json
        :param apply_schema_to_output:  whether to apply schema to output
        :param ignore_views_for_output: don't save these views
        :param output_folder: name of the output folder
        :param output_schema_folder: name of the folder containing output schema
        :param output_schema: the schema of the output (overwrites output_schema_folder)
        """
        self.func_path_modifier = func_path_modifier
        self.sort_output_by = sort_output_by
        self.output_as_json_only = output_as_json_only
        self.apply_schema_to_output = apply_schema_to_output
        self.ignore_views_for_output = ignore_views_for_output
        self.output_schema = output_schema
        self.output_schema_folder = output_schema_folder
        self.output_folder = output_folder
        self.related_inputs = (
            related_inputs
            if not related_inputs or isinstance(related_inputs, list)
            else [related_inputs]
        )
        # init in validate
        self.spark_session: SparkSession
        self.test_path: Optional[Path] = None
        self.output_folder_path: Path
        self.temp_folder_path: Optional[Path] = None
        self.input_table_names: Optional[List[str]] = None
        self.auto_sort: Optional[bool] = auto_sort

    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        """
        Validates the output files from a feature or pipeline


        :param test_name: the name of the test
        :param test_path: the path to the test
        :param spark_session: the spark session
        :param temp_folder_path: the path to the temp folder
        :param logger: the logger
        :param mock_client: the mock client
        :return: None
        """
        assert spark_session
        self.logger = logger

        self.input_table_names = []
        if self.related_inputs:
            self.input_table_names = self.related_inputs[0].input_table_names  # type: ignore
        self.spark_session = spark_session
        self.test_path = test_path
        self.output_folder_path = test_path.joinpath(self.output_folder)
        self.temp_folder_path = temp_folder_path

        # write out any missing output schemas
        output_schema_folder: Path = Path(self.test_path).joinpath(
            self.output_schema_folder
        )
        output_tables_for_writing_schema: List[str] = [
            table_name
            for table_name in spark_list_catalog_table_names(spark_session)
            if not table_name.startswith("expected_")
            and table_name not in self.input_table_names
        ]
        if self.ignore_views_for_output is not None:
            output_tables_for_writing_schema = [
                table
                for table in output_tables_for_writing_schema
                if table not in self.ignore_views_for_output
            ]

        if (
            "output" in output_tables_for_writing_schema
        ):  # if there is an output table then ignore other output tables
            output_tables_for_writing_schema = ["output"]
        if not self.output_schema:
            for table_name in output_tables_for_writing_schema:
                write_schema_to_output(
                    spark_session=self.spark_session,
                    view_name=table_name,
                    schema_folder=output_schema_folder,
                )
        # for each file in output folder, loading into a view in Spark (prepend with "expected_")

        if not os.path.exists(self.output_folder_path):
            os.mkdir(self.output_folder_path)
        # noinspection PyTypeChecker
        output_files: List[str] = [
            f
            for f in listdir(self.output_folder_path)
            if isfile(join(self.output_folder_path, f))
        ]
        views_found: List[str] = []
        data_frame_exceptions: List[SparkDataFrameComparerException] = []
        for output_file in output_files:
            found_output_file: bool
            data_frame_exception: Optional[SparkDataFrameComparerException]
            (
                found_output_file,
                data_frame_exception,
            ) = self.process_output_file(
                output_file=output_file,
                output_schema_folder=output_schema_folder,
                func_path_modifier=self.func_path_modifier,
                sort_output_by=self.sort_output_by,
                apply_schema_to_output=self.apply_schema_to_output,
                output_schema=self.output_schema,
                auto_sort=self.auto_sort,
            )
            if found_output_file:
                views_found.append(get_view_name_from_file_path(output_file).lower())
                if data_frame_exception:
                    data_frame_exceptions.append(data_frame_exception)
        # write out any missing output files
        table_names_to_write_to_output: List[str] = [
            table_name
            for table_name in spark_list_catalog_table_names(spark_session)
            if table_name.lower() not in views_found
            and not table_name.startswith("expected_")
            and table_name not in self.input_table_names
        ]
        if (
            "output" in table_names_to_write_to_output
        ):  # if there is an output table then ignore other output tables
            table_names_to_write_to_output = ["output"]
        for table_name in table_names_to_write_to_output:
            self.write_table_to_output(
                view_name=table_name,
                sort_output_by=self.sort_output_by,
                output_as_json_only=self.output_as_json_only,
            )
        if len(data_frame_exceptions) > 0:
            raise SparkPipelineFrameworkTestingException(
                exceptions=data_frame_exceptions
            )

    def process_output_file(
        self,
        *,
        output_file: str,
        output_schema_folder: Path,
        func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]],
        sort_output_by: Optional[List[str]],
        apply_schema_to_output: bool,
        output_schema: Optional[Union[StructType, Dict[str, StructType], DataType]],
        auto_sort: Optional[bool] = None,
    ) -> Tuple[bool, SparkDataFrameComparerException | None]:
        """
        read predefined outputs and compare them with the current outputs
        write outputs to disk if it doesn't exist


        :param output_file: the output file
        :param output_schema_folder: the folder containing the output schema
        :param func_path_modifier: function to modify the path
        :param sort_output_by: order of column names [col1, col2,...]
        :param apply_schema_to_output: apply schema to output
        :param output_schema: the schema of the output
        :param auto_sort: auto sort the output

        :return: found_output_file, data_frame_exception
        """
        assert self.spark_session

        data_frame_exception: Optional[SparkDataFrameComparerException] = None
        file_extension: str = get_file_extension_from_file_path(output_file)
        view_name: str = get_view_name_from_file_path(output_file)
        if file_extension.lower() not in [
            ".csv",
            ".json",
            ".jsonl",
            ".ndjson",
            ".parquet",
        ]:
            return True, data_frame_exception
        result_df: DataFrame = self.spark_session.table(view_name)
        sort_columns: List[str] = (
            [col for col in sort_output_by if col in result_df.columns]
            if sort_output_by
            else []
        )

        found_output_file: bool
        output_file_path = self.output_folder_path.joinpath(output_file)
        result_path: Optional[Path] = (
            self.temp_folder_path.joinpath(f"{view_name}")
            if self.temp_folder_path
            else None
        )

        # get schema
        output_schema_for_view: Optional[StructType] = None
        output_schema_file: str = os.path.join(
            output_schema_folder, f"{view_name}.json"
        )
        # if there is a schema file and no schema was passed in then use that
        if (
            apply_schema_to_output
            and not output_schema
            and os.path.exists(output_schema_file)
        ):
            with open(output_schema_file) as file:
                schema_json = json.loads(file.read())
            output_schema_for_view = StructType.fromJson(schema_json)
            self.logger.info(
                f"Reading file {output_file_path} using schema: {output_schema_file}"
            )
        elif apply_schema_to_output and output_schema:
            self.logger.info(
                f"Reading file {output_file_path} using passed in schema for view {view_name}"
            )
            # the schema is passed either as a single schema or a dict of schemas
            output_schema_for_view = (
                output_schema[view_name]  # type: ignore
                if output_schema
                and isinstance(output_schema, dict)
                and view_name in output_schema
                else output_schema
            )

        # create a reader to read the file (using schema if specified)
        reader = (
            self.spark_session.read.schema(output_schema_for_view)
            if output_schema_for_view
            else self.spark_session.read
        )

        # now read the file using the reader for the file extension
        output_df: DataFrame
        if file_extension.lower() == ".csv":
            output_df = reader.csv(
                path=str(output_file_path), header=True, comment="#", emptyValue=None
            )
            found_output_file = True
        elif (
            file_extension.lower() == ".jsonl"
            or file_extension.lower() == ".json"
            or file_extension.lower() == ".ndjson"
        ):
            output_df = reader.option("multiLine", True).json(
                path=str(output_file_path)
            )
            found_output_file = True
        elif file_extension.lower() == ".parquet":
            output_df = reader.parquet(path=str(output_file_path))
            found_output_file = True
        else:
            assert False, f"Unsupported file extension: {file_extension}"

        # create expected view
        output_df.createOrReplaceTempView(f"expected_{view_name}")

        # write result to temp folder for comparison
        if result_path and self.temp_folder_path:
            result_path_for_view = result_path.joinpath(f"{view_name}.json")
            result_df = (
                result_df.coalesce(1).sort(*sort_columns)
                if len(sort_columns) > 0
                else result_df.coalesce(1)
            )
            result_df.write.json(path=str(result_path_for_view))
            if output_schema and output_schema_for_view:
                result_df = result_df.sql_ctx.read.schema(output_schema_for_view).json(
                    str(result_path_for_view)
                )
            result_file: Path = self.temp_folder_path.joinpath(f"{view_name}.json")
            if file_extension.lower() == ".csv":
                self.combine_spark_csv_files_to_one_file(
                    source_folder=result_path_for_view,
                    destination_file=result_file,
                    file_extension="csv",
                )
            elif (
                file_extension.lower() == ".jsonl"
                or file_extension.lower() == ".json"
                or file_extension.lower() == ".ndjson"
            ):
                self.combine_spark_json_files_to_one_file(
                    source_folder=result_path_for_view,
                    destination_file=result_file,
                    file_extension="json",
                )

            if found_output_file:
                # Do a data frame compare on each view
                self.logger.info(
                    f"Comparing with view:[view_name= with view:[expected_{view_name}]"
                )
                try:
                    # drop any corrupted column
                    assert_compare_data_frames(
                        expected_df=self.spark_session.table(
                            f"expected_{view_name}"
                        ).drop("_corrupt_record"),
                        result_df=result_df,
                        result_path=result_file,
                        expected_path=output_file_path,
                        temp_folder=self.temp_folder_path,
                        func_path_modifier=func_path_modifier,
                        order_by=sort_columns if len(sort_columns) > 0 else None,
                        auto_sort=auto_sort,
                    )
                except SparkDataFrameComparerException as e:
                    data_frame_exception = e
                    # for schema errors, show a compare path for schema
                    if e.exception_type == ExceptionType.SchemaMismatch:
                        if self.temp_folder_path and output_schema_file:
                            # write the new schema to temp folder
                            result_schema_path = write_schema_to_output(
                                spark_session=self.spark_session,
                                view_name=view_name,
                                schema_folder=self.temp_folder_path.joinpath("schemas")
                                .joinpath("result")
                                .joinpath(view_name),
                            )
                            e.compare_path = self.get_compare_path(
                                result_path=result_schema_path,
                                expected_path=Path(output_schema_file),
                                temp_folder=self.temp_folder_path,
                                func_path_modifier=func_path_modifier,
                                type_="schema",
                            )
                            if func_path_modifier and e.compare_path:
                                e.compare_path = func_path_modifier(e.compare_path)

        return found_output_file, data_frame_exception

    def write_table_to_output(
        self,
        *,
        view_name: str,
        sort_output_by: Optional[List[str]],
        output_as_json_only: bool,
    ) -> None:
        assert self.temp_folder_path
        assert self.spark_session
        assert self.output_folder_path

        df: DataFrame = self.spark_session.table(view_name)
        sort_columns: List[str] = (
            [col for col in sort_output_by if col in df.columns]
            if sort_output_by
            else []
        )
        if output_as_json_only or self.should_write_dataframe_as_json(df=df):
            # save as json
            file_path: Path = self.temp_folder_path.joinpath(f"{view_name}.json")
            self.logger.info(f"Writing {file_path}")
            if len(sort_columns) > 0:
                df.coalesce(1).sort(*sort_columns).write.mode("overwrite").json(
                    path=str(file_path)
                )
            else:
                df.coalesce(1).write.mode("overwrite").json(path=str(file_path))
            self.combine_spark_json_files_to_one_file(
                source_folder=file_path,
                destination_file=self.output_folder_path.joinpath(f"{view_name}.json"),
                file_extension="json",
            )
        else:
            # save as csv
            file_path = self.temp_folder_path.joinpath(f"{view_name}.csv")
            self.logger.info(f"Writing {file_path}")

            if len(sort_columns) > 0:
                df.coalesce(1).sort(*sort_columns).write.mode("overwrite").csv(
                    path=str(file_path), header=True
                )
            else:
                df.coalesce(1).write.mode("overwrite").csv(
                    path=str(file_path), header=True
                )
            self.combine_spark_csv_files_to_one_file(
                source_folder=file_path,
                destination_file=self.output_folder_path.joinpath(f"{view_name}.csv"),
                file_extension="csv",
            )

    @staticmethod
    def should_write_dataframe_as_json(df: DataFrame) -> bool:
        types: List[Tuple[str, Any]] = df.dtypes
        type_dict: Dict[str, Any] = {key: value for key, value in types}
        # these type strings can look like 'array<struct<Field:string>>', so we
        # have to check if "array" or "struct" appears in the type string, not
        # just for exact matches
        return any([t for t in type_dict.values() if "array" in t or "struct" in t])

    @staticmethod
    def combine_spark_csv_files_to_one_file(
        source_folder: Path, destination_file: Path, file_extension: str
    ) -> None:
        file_pattern_to_search: Path = source_folder.joinpath(f"*.{file_extension}")
        # find files with that extension in source_folder
        files: List[str] = glob(str(file_pattern_to_search))
        lines: List[str] = []
        for file in files:
            with open(file, "r") as file_source:
                lines = lines + file_source.readlines()

        with open(destination_file, "w") as file_destination:
            file_destination.writelines(lines)
            file_destination.write("\n")

    @staticmethod
    def combine_spark_json_files_to_one_file(
        source_folder: Path, destination_file: Path, file_extension: str
    ) -> None:
        file_pattern_to_search: Path = source_folder.joinpath(f"*.{file_extension}")
        # find files with that extension in source_folder
        files: List[str] = glob(str(file_pattern_to_search))
        # now copy the first file to the destination
        lines: List[str] = []
        for file in files:
            with open(file, "r") as file_source:
                lines = lines + file_source.readlines()

        # convert from json to json and write in pretty print
        os.makedirs(os.path.dirname(destination_file), exist_ok=True)
        with open(destination_file, "w") as file_destination:
            json_array: List[Any] = [json.loads(line) for line in lines]
            file_destination.write(json.dumps(json_array, indent=2))
            file_destination.write("\n")

    @staticmethod
    def get_compare_path(
        result_path: Optional[Path],
        expected_path: Optional[Path],
        temp_folder: Optional[Union[Path, str]],
        func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]],
        type_: str,
    ) -> Optional[Path]:
        compare_sh_path: Optional[Path] = None
        if expected_path and result_path and temp_folder:
            expected_file_name: str = os.path.basename(expected_path)
            # create a temp file to launch the diff tool
            # use .command:
            # https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
            compare_sh_path = Path(temp_folder).joinpath(
                f"compare_{type_}_{expected_file_name}.command"
            )
            with open(compare_sh_path, "w") as compare_sh:
                compare_sh.write(
                    'open -na "PyCharm.app" --args diff '
                    f"{func_path_modifier(result_path) if func_path_modifier else result_path}"
                    f" {func_path_modifier(expected_path) if func_path_modifier else expected_path}"
                )
                os.fchmod(compare_sh.fileno(), 0o7777)
        return compare_sh_path
