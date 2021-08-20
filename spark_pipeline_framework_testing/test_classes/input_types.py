import glob
import json
import os
from abc import ABC, abstractmethod
from os import listdir
from os.path import isfile, join, isdir
from pathlib import Path
from typing import List, Optional, Dict, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Table
from pyspark.sql.types import StructType, DataType
from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore
from spark_pipeline_framework.utilities.json_to_jsonl_converter import (
    convert_json_to_jsonl,
)

from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    request,
    response,
    times,
)
from spark_pipeline_framework_testing.tests_common.common_functions import (
    get_view_name_from_file_path,
    get_file_extension_from_file_path,
    write_schema_to_output,
)
from spark_pipeline_framework_testing.tests_common.mock_requests_loader import (
    load_mock_fhir_requests_from_folder,
)
from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)


class TestInputType(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def initialize(
        self,
        test_name: str,
        test_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        pass

    @staticmethod
    def raise_if_not_exist(path: Path) -> None:
        if not path.exists():
            raise Exception(
                f"input path '{path}' was not found! Please create the dir and put the input files in it."
            )


class FhirCalls(TestInputType):
    """
    This class mocks and/or validates calls to a Fhir Server
    https://www.hl7.org/fhir/summary.html#:~:text=FHIR%C2%AE%20%E2%80%93%20Fast%20Healthcare%20Interoperability,a%20tight%20focus%20on%20implementability.
    """

    def __init__(
        self,
        fhir_validation_url: str = "http://fhir:3000/4_0_0",
        fhir_calls_folder: str = "fhir_calls",
        mock_url_prefix: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.fhir_calls_folder = fhir_calls_folder
        self.fhir_validation_url = fhir_validation_url
        self.url_prefix = mock_url_prefix

        self.test_name: str
        self.test_path: Path
        self.mock_client: MockServerFriendlyClient
        self.spark_session: SparkSession
        self.temp_folder_path: Path
        self.mocked_files: Optional[
            List[str]
        ] = None  # list of files that are used in mocking

    def initialize(
        self,
        test_name: str,
        test_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        assert mock_client
        assert spark_session
        self.test_name = test_name
        self.test_path = test_path
        self.logger = logger
        self.mock_client = mock_client
        self.spark_session = spark_session
        self.temp_folder_path = test_path.joinpath(self.test_path)
        if self.url_prefix is None:
            self.url_prefix = test_name
        fhir_calls_path: Path = self.test_path.joinpath(self.fhir_calls_folder)
        self.raise_if_not_exist(fhir_calls_path)
        self._run_mocked_fhir_test()

    def _run_mocked_fhir_test(self) -> None:
        self.mocked_files = load_mock_fhir_requests_from_folder(
            folder=self.test_path.joinpath(self.fhir_calls_folder),
            mock_client=self.mock_client,
            url_prefix=self.url_prefix,
        )

        self.mock_client.expect_default()


class FileInput(TestInputType):
    """
    This class loads standard files to a Spark view
    """

    def __init__(
        self,
        test_input_folder: str = "input",
        input_schema_folder: str = "input_schema",
        input_schema: Optional[
            Union[StructType, Dict[str, StructType], DataType]
        ] = None,
        row_limit: int = 100,
    ) -> None:
        """
        :param test_input_folder: name of the folder in the test directory
        :param input_schema_folder: name of the folder containing the schema of the input
        :param input_schema: schema of the input file (overwrites input_schema_folder)
        :param row_limit: row limit for input file
        """
        super().__init__()
        self.test_input_folder = test_input_folder
        self.input_schema = input_schema
        self.input_schema_folder = input_schema_folder
        self.test_path: Path
        self.row_limit = row_limit
        self.input_table_names: List[str]
        self.spark_session: SparkSession

    def initialize(
        self,
        test_name: str,
        test_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        assert spark_session
        self.spark_session = spark_session
        self.test_path = test_path
        self.logger = logger
        input_table_names = self.ingest_input_files(self.input_schema)
        self.input_table_names = input_table_names

    def ingest_input_files(
        self, input_schema: Optional[Union[StructType, Dict[str, StructType], DataType]]
    ) -> List[str]:
        print(f"Running test in folder: {self.test_path}...")

        input_folder: Path = Path(self.test_path).joinpath(self.test_input_folder)
        if not input_folder.exists():
            raise
        input_files: List[str] = []
        if isdir(input_folder):
            input_files = [
                f for f in listdir(input_folder) if isfile(join(input_folder, f))
            ]
        input_schema_folder = Path(self.test_path).joinpath(self.input_schema_folder)

        # for each file in input folder, load into a view in Spark
        #   (use name of file without extension as name of view)
        for input_file in input_files:
            self.read_input_file(
                input_file=input_file,
                input_folder=input_folder,
                input_schema_folder=input_schema_folder,
                input_schema=input_schema,
            )

        # get list of input tables
        all_tables: List[Table] = self.spark_session.catalog.listTables("default")
        input_table_names: List[str] = [
            t.name
            for t in all_tables
            if not t.name.startswith("expected_")  # it's an output table
        ]
        # write out any input schemas if it's not explicitly defined
        if not input_schema:
            table_name: str
            for table_name in input_table_names:
                if not os.path.exists(input_schema_folder):
                    os.mkdir(input_schema_folder)

                write_schema_to_output(
                    spark_session=self.spark_session,
                    view_name=table_name,
                    schema_folder=input_schema_folder,
                )
        return input_table_names

    def read_input_file(
        self,
        input_file: str,
        input_folder: Path,
        input_schema_folder: Path,
        input_schema: Optional[Union[StructType, Dict[str, StructType], DataType]],
    ) -> None:
        assert self.spark_session

        file_extension: str = get_file_extension_from_file_path(input_file)
        if file_extension.lower() not in [".csv", ".json", ".jsonl", ".parquet"]:
            return  # todo: issue a warning

        view_name: str = get_view_name_from_file_path(input_file)

        input_file_path = os.path.join(input_folder, input_file)

        # get schema
        input_schema_for_view: Optional[StructType] = None
        input_schema_file: str = os.path.join(input_schema_folder, f"{view_name}.json")
        # if there is a schema file and no schema was passed in then use that
        if not input_schema and os.path.exists(input_schema_file):
            with open(input_schema_file, "r") as file:
                schema_json = json.loads(file.read())
            input_schema_for_view = StructType.fromJson(schema_json)
            print(f"Reading file {input_file_path} using schema: {input_schema_file}")
        elif input_schema:
            print(
                f"Reading file {input_file_path} using passed in schema for view {view_name}"
            )
            # the schema is passed either as a single schema or a dict of schemas
            input_schema_for_view = (
                input_schema[view_name]  # type: ignore
                if input_schema
                and isinstance(input_schema, dict)
                and view_name in input_schema
                else input_schema
            )

        # create a reader to read the file (using schema if specified)
        reader = (
            self.spark_session.read.schema(input_schema_for_view)
            if input_schema_for_view
            else self.spark_session.read
        )

        input_df: DataFrame
        if file_extension.lower() == ".csv":
            input_df = reader.csv(
                path=input_file_path,
                header=True,
                comment="#",
                emptyValue=None,
            ).limit(self.row_limit)
        elif file_extension.lower() == ".jsonl" or file_extension.lower() == ".json":
            # create json_input_folder if it does not exist
            json_input_folder = os.path.join(input_folder, "..", "input_jsonl")
            if not os.path.exists(json_input_folder):
                os.mkdir(json_input_folder)
            jsonl_input_file_path = os.path.join(json_input_folder, input_file)
            # convert file to jsonl if needed
            convert_json_to_jsonl(
                src_file=Path(input_file_path), dst_file=Path(jsonl_input_file_path)
            )
            input_df = reader.json(path=jsonl_input_file_path).limit(self.row_limit)
        elif file_extension.lower() == ".parquet":
            input_df = reader.parquet(path=input_file_path).limit(self.row_limit)
        else:
            assert (
                False
            ), f"Unsupported file extension: {file_extension}"  # todo: unreachable condition

        # create expected view
        input_df.createOrReplaceTempView(view_name)
        assert (
            "_corrupt_record" not in self.spark_session.table(view_name).columns
        ), input_file_path


class HttpJsonRequest(TestInputType):
    """
    This class help mock a http call using this format:
    {
      "request_parameters": {
        "method": "<method name>",
        "querystring": {
        ...
        }
      },
      "request_result": {
      ....
      }
    }
    """

    def __init__(
        self,
        response_data_folder: str = "source_api_calls",
        mock_url_prefix: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.response_data_folder = response_data_folder
        self.test_path: Path
        self.url_prefix = mock_url_prefix

    def initialize(
        self,
        test_name: str,
        test_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        assert mock_client
        self.test_path = test_path
        self.logger = logger
        if self.url_prefix is None:
            self.url_prefix = test_name
        response_data_path = self.test_path.joinpath(self.response_data_folder)
        self.raise_if_not_exist(response_data_path)
        self.load_mock_source_api_responses_from_folder(
            folder=response_data_path,
            mock_client=mock_client,
            url_prefix=self.url_prefix,
        )

    @staticmethod
    def load_mock_source_api_responses_from_folder(
        folder: Path, mock_client: MockServerFriendlyClient, url_prefix: Optional[str]
    ) -> List[str]:
        """
        Mock responses for all files from the folder and its sub-folders

        from https://pypi.org/project/mockserver-friendly-client/

        :param folder: where to look for files (recursively)
        :param mock_client:
        :param url_prefix:
        """
        file_path: str
        files: List[str] = sorted(
            glob.glob(str(folder.joinpath("**/*")), recursive=True)
        )
        for file_path in files:
            with open(file_path, "r") as file:
                content = file.read()
                path = f"{('/' + url_prefix) if url_prefix else ''}/{os.path.basename(file_path)}"
                mock_client.expect(
                    request(
                        method="GET",
                        path=path,
                    ),
                    response(body=content),
                    timing=times(1),
                )
                print(f"Mocking: GET {mock_client.base_url}{path}")
        return files


class ApiJsonResponse(TestInputType):
    """
    Mock responses for all files from the folder and its sub-folders
    """

    def __init__(
        self,
        response_data_folder: str = "api_json_response",
        mock_url_prefix: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.input_folder_name = response_data_folder
        self.url_prefix = mock_url_prefix

    def initialize(
        self,
        test_name: str,
        test_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        assert mock_client
        if self.url_prefix is None:
            self.url_prefix = test_name
        expected_input_path = test_path.joinpath(self.input_folder_name)
        self.raise_if_not_exist(expected_input_path)
        self.load_mock_source_api_json_responses(
            folder=expected_input_path,
            mock_client=mock_client,
            url_prefix=self.url_prefix,
            add_file_name=True,
        )

    @staticmethod
    def load_mock_source_api_json_responses(
        folder: Path,
        mock_client: MockServerFriendlyClient,
        url_prefix: Optional[str],
        add_file_name: bool = False,
    ) -> List[str]:
        """
        Mock responses for all files from the folder and its sub-folders

        :param folder: where to look for files (recursively)
        :param mock_client:
        :param url_prefix: http://{mock_server_url}/{url_prefix}...
        :param add_file_name: http://{mock_server_url}/{url_prefix}/{add_file_name}...
        """
        file_path: str
        files: List[str] = sorted(
            glob.glob(str(folder.joinpath("**/*.json")), recursive=True)
        )
        for file_path in files:
            file_name = os.path.basename(file_path)
            with open(file_path, "r") as file:
                content = json.loads(file.read())

                try:
                    request_parameters = content["request_parameters"]
                except ValueError:
                    raise Exception(
                        "`request_parameters` key not found! It is supposed to contain parameters of the request function."
                    )

                path = f"{('/' + url_prefix) if url_prefix else ''}"
                path = (
                    f"{path}/{os.path.splitext(file_name)[0]}"
                    if add_file_name
                    else path
                )

                try:
                    request_result = content["request_result"]
                except ValueError:
                    raise Exception(
                        "`request_result` key not found. It is supposed to contain the expected result of the requst function."
                    )
                mock_client.expect(
                    request(path=path, **request_parameters),
                    response(body=json.dumps(request_result)),
                    timing=times(1),
                )
                print(f"Mocking {mock_client.base_url}{path}: {request_parameters}")
        return files
