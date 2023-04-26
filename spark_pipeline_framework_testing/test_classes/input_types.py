import json
import os
from abc import ABC, abstractmethod
from os import listdir, makedirs
from os.path import isfile, join, isdir
from pathlib import Path
from typing import List, Optional, Dict, Union, Any

from mockserver_client.mock_requests_loader import (
    load_mock_fhir_requests_from_folder,
    load_mock_fhir_requests_for_single_file,
    load_mock_source_api_json_responses,
)
from mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Table
from pyspark.sql.functions import trim, col
from pyspark.sql.types import StructType, DataType
from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore
from spark_pipeline_framework.transformers.framework_fixed_width_loader.v1.framework_fixed_width_loader import (
    ColumnSpec,
)
from spark_pipeline_framework.utilities.json_to_jsonl_converter.json_to_jsonl_converter import (
    convert_json_to_jsonl,
)

from spark_pipeline_framework_testing.tests_common.common_functions import (
    get_view_name_from_file_path,
    get_file_extension_from_file_path,
    write_schema_to_output,
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

    @staticmethod
    def create_if_not_exist(path: Path) -> None:
        if not path.exists():
            makedirs(path, exist_ok=True)


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
        method: str = "POST",
        relative_path: Optional[str] = None,
        query_string: Optional[Dict[str, Any]] = None,
        from_folder: Optional[bool] = True,
        single_file_name: Optional[str] = None,
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
        ] = []  # list of files that are used in mocking
        self.method: str = method
        self.relative_path: Optional[str] = relative_path
        self.query_string: Optional[Dict[str, Any]] = query_string
        self.from_folder: Optional[bool] = from_folder
        self.single_file_name: Optional[str] = single_file_name

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
        self.create_if_not_exist(fhir_calls_path)
        self._run_mocked_fhir_test()

    def _run_mocked_fhir_test(self) -> None:
        if self.from_folder:
            self.mocked_files = load_mock_fhir_requests_from_folder(
                folder=self.test_path.joinpath(self.fhir_calls_folder),
                mock_client=self.mock_client,
                method=self.method,
                url_prefix=self.url_prefix,
                relative_path=self.relative_path,
                query_string=self.query_string,
            )
        else:
            self.mocked_files = load_mock_fhir_requests_for_single_file(
                folder=self.test_path.joinpath(self.fhir_calls_folder),
                single_file_name=str(self.single_file_name),
                mock_client=self.mock_client,
                method=self.method,
                url_prefix=self.url_prefix,
                relative_path=self.relative_path,
                query_string=self.query_string,
            )


class FileInput(TestInputType):
    """
    This class loads standard files to a Spark view
    """

    def __init__(
        self,
        test_input_folder: str = "input",
        input_schema_folder: str = "input_schema",
        input_schema: Optional[
            Union[StructType, Dict[str, StructType], Dict[str, DataType], DataType]
        ] = None,
        row_limit: int = 100,
        row_tag: Optional[str] = None,
        fixed_width_columns: Optional[List[ColumnSpec]] = None,
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
        self.row_tag = row_tag
        self.fixed_width_columns = fixed_width_columns

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
        self,
        input_schema: Optional[
            Union[StructType, Dict[str, StructType], Dict[str, DataType], DataType]
        ],
    ) -> List[str]:
        print(f"Running test in folder: {self.test_path}...")

        input_folder: Path = Path(self.test_path).joinpath(self.test_input_folder)
        if not input_folder.exists():
            raise
        input_files: List[str] = []
        if isdir(input_folder):
            # noinspection PyTypeChecker
            input_files = [
                f
                for f in listdir(input_folder)
                if isfile(join(input_folder, f))
                and not f.endswith(".py")
                and get_file_extension_from_file_path(f) != ""
                and get_file_extension_from_file_path(f) is not None
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
        input_schema: Optional[
            Union[StructType, Dict[str, StructType], Dict[str, DataType], DataType]
        ],
    ) -> None:
        assert self.spark_session

        file_extension: str = get_file_extension_from_file_path(input_file)
        if file_extension.lower() not in [
            ".csv",
            ".json",
            ".jsonl",
            ".parquet",
            ".txt",
            ".xml",
        ]:
            assert (
                False
            ), f"Unsupported file extension: {file_extension} for file {input_file}. Extend input_types.read_input_file to handle {file_extension} files."

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
        elif file_extension.lower() == ".txt":
            input_df = reader.text(paths=input_file_path).limit(self.row_limit)
            if self.fixed_width_columns:
                input_df = input_df.select(
                    *[
                        trim(col("value").substr(column.start_pos, column.length))
                        .cast(column.data_type)
                        .alias(column.column_name)
                        for column in self.fixed_width_columns
                    ]
                )
        elif file_extension.lower() == ".xml":
            input_df = (
                reader.format("xml")
                .options(rowTag=self.row_tag)
                .load(path=input_file_path)
                .limit(self.row_limit)
            )
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
        response_data_folder: str = "request_json_calls",
        mock_url_prefix: Optional[str] = None,
        add_file_name_to_request: bool = True,
    ) -> None:
        super().__init__()
        self.response_data_folder = response_data_folder
        self.test_path: Path
        self.url_prefix = mock_url_prefix
        self.add_file_name_to_request = add_file_name_to_request

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
        load_mock_source_api_json_responses(
            folder=response_data_path,
            mock_client=mock_client,
            url_prefix=self.url_prefix,
            add_file_name=self.add_file_name_to_request,
        )


class MockFhirRequest(TestInputType):
    def __init__(
        self,
        fhir_calls_folder: str,
        fhir_resource_type: Optional[str] = None,
        fhir_endpoint: Optional[str] = None,
    ) -> None:
        """
        :param fhir_calls_folder: the folder name with the requests to mock
        :param fhir_resource_type: the fhir resource name being mocked. generally used when mocking a GET request for a single fhir resource
        :param fhir_endpoint: set this when doing a request such as $graph
        """
        super().__init__()
        self.fhir_calls_folder = fhir_calls_folder
        self.test_path: Path
        self.fhir_resource_type = fhir_resource_type
        self.fhir_endpoint = fhir_endpoint
        self.mocked_files: Optional[
            List[str]
        ] = []  # list of files that are used in mocking

    def initialize(
        self,
        test_name: str,
        test_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        assert mock_client
        url_prefix = f"{test_name}/4_0_0"
        if self.fhir_resource_type:
            url_prefix = f"{url_prefix}/{self.fhir_resource_type}"
        response_data_path = test_path.joinpath(self.fhir_calls_folder)
        self.raise_if_not_exist(response_data_path)
        self.mocked_files = load_mock_source_api_json_responses(
            folder=response_data_path,
            mock_client=mock_client,
            url_prefix=url_prefix,
            add_file_name=True,
            url_suffix=self.fhir_endpoint,
        )
