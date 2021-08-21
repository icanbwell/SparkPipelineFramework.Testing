import glob
import json
import os
from abc import ABC, abstractmethod
from os import listdir
from os.path import isfile, join
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, Union, Callable, TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Table
from pyspark.sql.types import StructType, DataType
from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
    ExceptionType,
)
from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore

from spark_pipeline_framework_testing.mockserver_client.exceptions.mock_server_expectation_not_found_exception import (
    MockServerExpectationNotFoundException,
)
from spark_pipeline_framework_testing.mockserver_client.exceptions.mock_server_json_content_mismatch_exception import (
    MockServerJsonContentMismatchException,
)
from spark_pipeline_framework_testing.mockserver_client.exceptions.mock_server_request_not_found_exception import (
    MockServerRequestNotFoundException,
)
from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)
from spark_pipeline_framework_testing.tests_common.path_converter import (
    convert_path_from_docker,
)
from spark_pipeline_framework_testing.mockserver_client.mockserver_verify_exception import (
    MockServerVerifyException,
)

if TYPE_CHECKING:
    from spark_pipeline_framework_testing.test_classes.input_types import (
        FhirCalls,
        FileInput,
    )
from spark_pipeline_framework_testing.testing_exception import (
    SparkPipelineFrameworkTestingException,
)
from spark_pipeline_framework_testing.tests_common.common_functions import (
    write_schema_to_output,
    get_view_name_from_file_path,
    clean_spark_session,
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


class MockCallValidator(Validator):
    """
    validates Mock calls
    """

    def __init__(
        self,
        related_inputs: Optional[Union[List["FhirCalls"], "FhirCalls"]],
    ) -> None:
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
        assert MockServerFriendlyClient
        assert mock_client
        self.logger = logger
        data_folder_path: Path = test_path.joinpath(
            self.related_inputs[0].fhir_calls_folder
        )
        try:
            mock_client.verify_expectations(
                test_name=test_name, files=self.get_input_files()
            )
        except MockServerVerifyException as e:
            compare_files: List[str] = []
            for exception in e.exceptions:
                if isinstance(exception, MockServerJsonContentMismatchException):
                    expected_path = exception.expected_file_path
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
                        with open(result_path, "w") as file_result:
                            file_result.write(json.dumps(exception.actual, indent=2))
                        with open(compare_sh_path, "w") as compare_sh:
                            compare_sh.write(
                                f"/usr/local/bin/charm diff "
                                f"{convert_path_from_docker(result_path) if convert_path_from_docker else result_path} "
                                f"{convert_path_from_docker(expected_path) if convert_path_from_docker else expected_path}"
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
                        assert "resourceType" in resource_obj, resource_obj
                        resource_type = resource_obj["resourceType"]
                        assert "id" in resource_obj, resource_obj
                        resource_id = resource_obj["id"]
                        resource_type_folder_name: str = camel_case_to_snake_case(
                            resource_type
                        )
                        resource_path: Path = data_folder_path.joinpath(
                            f"{resource_type_folder_name}"
                        )
                        os.makedirs(resource_path, exist_ok=True)
                        resource_file_path: Path = resource_path.joinpath(
                            f"{resource_id}.json"
                        )
                        with open(resource_file_path, "w") as file:
                            file.write(json.dumps(resource_obj, indent=2))
                        self.logger.info(
                            f"Writing http calls file to : {resource_file_path}"
                        )
                elif isinstance(exception, MockServerExpectationNotFoundException):
                    # add to error
                    pass
                else:
                    raise Exception(f"unknown exception type: {type(exception)}")
            for c in compare_files:
                self.logger.info(c)
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
                failure_message += f"{len(content_not_matched_exceptions)} files did not match: \n{compare_files_text}"
            expectations_not_met_exceptions: List[
                MockServerExpectationNotFoundException
            ] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerExpectationNotFoundException)
            ]
            if len(expectations_not_met_exceptions) > 0:
                failure_message += "\nExpectations not met:\n"
                failure_message += "\n".join(
                    [f"{c.url}: {c.json}" for c in expectations_not_met_exceptions]
                )

            unexpected_requests: List[MockServerRequestNotFoundException] = [
                c
                for c in e.exceptions
                if isinstance(c, MockServerRequestNotFoundException)
            ]
            if len(unexpected_requests) > 0:
                warning_message = ""
                warning_message += "\nUnexpected requests:\n"
                warning_message += "\n".join(
                    [f"{c.url}: {c.json_dict}" for c in unexpected_requests]
                )
                self.logger.info(warning_message)

            # if there is a failure then stop the test
            if failure_message:
                pytest.fail(failure_message)

    def get_input_files(self) -> List[str]:
        if not self.related_inputs:
            return []
        all_files: List[str] = []
        related_input: "FhirCalls"
        for related_input in self.related_inputs:
            if related_input:
                all_files.extend(related_input.mocked_files)  # type: ignore
        return all_files


class OutputFileValidator(Validator):
    """
    compare input and output files
    """

    def __init__(
        self,
        related_inputs: Union[List["FileInput"], "FileInput"],
        func_path_modifier: Optional[
            Callable[[Union[Path, str]], Union[Path, str]]
        ] = None,
        sort_output_by: Optional[List[str]] = None,
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

        :param related_inputs: the corresponding input for this validator
        :param func_path_modifier: in case you want to change paths e.g. docker to local
        :param sort_output_by: order of column names [col1, col2,...]
        :param output_as_json_only: save output as json
        :param apply_schema_to_output:
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

    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        assert spark_session
        assert self.related_inputs
        assert self.related_inputs[0].input_table_names  # type: ignore
        self.logger = logger

        self.input_table_names = self.related_inputs[0].input_table_names  # type: ignore
        self.spark_session = spark_session
        self.test_path = test_path
        self.output_folder_path = test_path.joinpath(self.output_folder)
        self.temp_folder_path = temp_folder_path

        # write out any missing output schemas
        output_tables: List[Table] = self.spark_session.catalog.listTables("default")
        if self.ignore_views_for_output is not None:
            output_tables = [
                table
                for table in output_tables
                if table.name not in self.ignore_views_for_output
            ]
        output_schema_folder: Path = Path(self.test_path).joinpath(
            self.output_schema_folder
        )
        output_tables_for_writing_schema: List[str] = [
            t.name
            for t in output_tables
            if not t.name.startswith("expected_")
            and t.name not in self.input_table_names
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
        output_files = [
            f
            for f in listdir(self.output_folder_path)
            if isfile(join(self.output_folder_path, f))
        ]
        views_found: List[str] = []
        data_frame_exceptions: List[SparkDataFrameComparerException] = []
        for output_file in output_files:
            found_output_file: bool
            data_frame_exception: Optional[SparkDataFrameComparerException]
            (found_output_file, data_frame_exception,) = self.process_output_file(
                output_file=output_file,
                output_schema_folder=output_schema_folder,
                func_path_modifier=self.func_path_modifier,
                sort_output_by=self.sort_output_by,
                apply_schema_to_output=self.apply_schema_to_output,
                output_schema=self.output_schema,
            )
            if found_output_file:
                views_found.append(get_view_name_from_file_path(output_file).lower())
                if data_frame_exception:
                    data_frame_exceptions.append(data_frame_exception)
        # write out any missing output files
        table_names_to_write_to_output: List[str] = [
            t.name
            for t in output_tables
            if t.name.lower() not in views_found
            and not t.name.startswith("expected_")
            and t.name not in self.input_table_names
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
        clean_spark_session(session=self.spark_session)
        if len(data_frame_exceptions) > 0:
            raise SparkPipelineFrameworkTestingException(
                exceptions=data_frame_exceptions
            )

    def process_output_file(
        self,
        output_file: str,
        output_schema_folder: Path,
        func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]],
        sort_output_by: Optional[List[str]],
        apply_schema_to_output: bool,
        output_schema: Optional[Union[StructType, Dict[str, StructType], DataType]],
    ) -> Tuple[bool, Optional[SparkDataFrameComparerException]]:
        """
        read predefined outputs and compare them with the current outputs
        write outputs to disk if doesn't exists

        """
        assert self.spark_session

        data_frame_exception: Optional[SparkDataFrameComparerException] = None
        file_extension: str = get_file_extension_from_file_path(output_file)
        view_name: str = get_view_name_from_file_path(output_file)
        if file_extension.lower() not in [".csv", ".json", ".jsonl", ".parquet"]:
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
        elif file_extension.lower() == ".jsonl" or file_extension.lower() == ".json":
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
                file_extension.lower() == ".jsonl" or file_extension.lower() == ".json"
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
        files: List[str] = glob.glob(str(file_pattern_to_search))
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
        files: List[str] = glob.glob(str(file_pattern_to_search))
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
                    f"/usr/local/bin/charm diff "
                    f"{func_path_modifier(result_path) if func_path_modifier else result_path} "
                    f"{func_path_modifier(expected_path) if func_path_modifier else expected_path}"
                )
                os.fchmod(compare_sh.fileno(), 0o7777)
        return compare_sh_path
