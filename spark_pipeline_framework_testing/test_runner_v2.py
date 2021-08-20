import json
import os
import shutil
from pathlib import Path
from re import search
from typing import List, Optional, Match, Dict, Any, Type, TYPE_CHECKING

import pytest
from helix_fhir_client_sdk.exceptions.fhir_sender_exception import FhirSenderException  # type: ignore
from pyspark.ml import Transformer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import PythonException
from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.proxy_base import ProxyBase
from spark_pipeline_framework.utilities.FriendlySparkException import (
    FriendlySparkException,
)
from spark_pipeline_framework.utilities.class_helpers import ClassHelpers

from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)

if TYPE_CHECKING:
    from spark_pipeline_framework_testing.test_classes.input_types import TestInputType
from spark_pipeline_framework_testing.test_classes.validator_types import (
    Validator,
)
from spark_pipeline_framework_testing.testing_exception import (
    SparkPipelineFrameworkTestingException,
)
from spark_pipeline_framework_testing.tests_common.fhir_sender_testing_exception_handler import (
    handle_fhir_sender_exception,
)
from spark_pipeline_framework_testing.tests_common.path_converter import (
    convert_path_from_docker,
)


class SparkPipelineFrameworkTestRunnerV2:
    row_limit: int = 100

    def __init__(
        self,
        spark_session: SparkSession,
        test_path: Path,
        test_name: str,
        test_validators: Optional[List[Validator]],
        logger: Logger,
        auto_find_helix_transformer: bool = True,
        helix_transformers: Optional[List[Type[ProxyBase]]] = None,
        mock_client: Optional[MockServerFriendlyClient] = None,
        fhir_server_url: Optional[str] = None,
        fhir_validation_url: Optional[str] = None,
        test_inputs: Optional[List["TestInputType"]] = None,
        temp_folder: Optional[str] = "temp",
        extra_params: Optional[Dict[str, Any]] = None,
        capture_exceptions: bool = True,
        helix_pipeline_parameters: Optional[Dict[str, Any]] = None,
        parameters_filename: str = "parameters.json",
    ) -> None:
        """
        :param auto_find_helix_transformer: find transformer based on the test location (overwrites helix_transformers)
        :param test_name: unique name for the test
        :param helix_transformers: list of the transformers to process the input
        :param test_validators: check if the output is what it's supposed to be
        :param mock_client: mocker server object
        :param fhir_server_url: to be sent to the transformer as param
        :param fhir_validation_url: to validate the fhir schema
        :param helix_pipeline_parameters: parameters necessary to run the pipeline
        :param test_inputs: inputs to the test e.g. file
        :param extra_params: in case extra information needs to be passed to the transformer
        :param capture_exceptions: for test purposes, set false if you want to capture exceptions on test side
        :param parameters_filename: name of the file containing the parameters (overwrites helix_pipeline_parameters)
        :param spark_session: Spark Session
        :param test_path: where to look for test files
        :param temp_folder: folder to use for temporary files.  Any existing files in this folder will be deleted.
        """

        self.test_path = test_path
        self.spark_session = spark_session
        self.helix_pipeline_parameters = helix_pipeline_parameters
        self.test_name = test_name
        self.test_validators = test_validators
        self.test_inputs = test_inputs
        self.helix_transformers = helix_transformers
        self.mock_client = mock_client
        self.auto_find_helix_transformer = auto_find_helix_transformer
        self.fhir_server_url = fhir_server_url
        self.fhir_validation_url = fhir_validation_url
        self.capture_exceptions = capture_exceptions
        self.logger = logger
        self.temp_folder_path = test_path.joinpath(temp_folder) if temp_folder else None
        if not self.temp_folder_path:
            self.temp_folder_path = self.test_path.joinpath("temp")
        self.extra_params = extra_params
        self.parameters_filename = parameters_filename
        # inject configs
        standard_parameters = dict(
            {
                "run_id": "foo",
                "source_filepath": test_path.joinpath("input"),
                "data_lake_path": self.temp_folder_path.joinpath("data_lake"),
                # "checkpoint_path": temp_folder.joinpath("checkpoint"),
                "fhir_server_url": fhir_server_url,
                "fhir_validation_url": fhir_validation_url,
                "athena_schema": "fake_schema",
                "cache_handler": self.extra_params.get("cache_handler")
                if self.extra_params
                else None,
                "address_standardization_class": self.extra_params.get(
                    "address_standardization_class"
                )
                if self.extra_params
                else None,
                "batch_size": 1,
            }
        )
        if self.helix_pipeline_parameters:
            standard_parameters.update(self.helix_pipeline_parameters)
            self.helix_pipeline_parameters = standard_parameters

    def run_test2(self) -> None:
        assert self.temp_folder_path
        try:
            os.environ["CATALOG_LOCATION"] = str(self.test_path)
            if self.temp_folder_path.exists():
                shutil.rmtree(self.temp_folder_path)

            test_input: "TestInputType"
            if self.test_inputs:
                for test_input in self.test_inputs:
                    test_input.initialize(
                        self.test_name,
                        self.test_path,
                        self.logger,
                        self.mock_client,
                        self.spark_session,
                    )
            if not self.auto_find_helix_transformer:
                if self.helix_transformers:
                    for transformer in self.helix_transformers:
                        self.run_helix_transformers(
                            parameters=self.helix_pipeline_parameters
                            if self.helix_pipeline_parameters
                            else {},
                            transformer_class=transformer,
                        )
            else:
                transformer_class = self.find_transformer(str(self.test_path))
                if transformer_class:
                    self.run_helix_transformers(
                        parameters=self.helix_pipeline_parameters
                        if self.helix_pipeline_parameters
                        else {},
                        transformer_class=transformer_class,
                    )

            if self.test_validators:
                test_validator: Validator
                for test_validator in self.test_validators:
                    test_validator.validate(
                        test_name=self.test_name,
                        test_path=self.test_path,
                        spark_session=self.spark_session,
                        temp_folder_path=self.temp_folder_path,
                        mock_client=self.mock_client,
                        logger=self.logger,
                    )

        except SparkPipelineFrameworkTestingException as e:
            if self.capture_exceptions:
                # https://docs.pytest.org/en/documentation-restructure/example/simple.html#writing-well-integrated-assertion-helpers
                pytest.fail(
                    ",".join(
                        [
                            str(data_frame_exception.compare_path)
                            for data_frame_exception in e.exceptions
                        ]
                    )
                )
            else:
                raise e
        except FriendlySparkException as e:
            # has PythonException from Spark
            if isinstance(e.exception, FhirSenderException):
                handle_fhir_sender_exception(
                    e=e,
                    temp_folder=self.temp_folder_path,
                    func_path_modifier=convert_path_from_docker,
                )
            elif (
                isinstance(e.exception, PythonException)
                and "FhirSenderException" in e.exception.desc
            ):
                handle_fhir_sender_exception(
                    e=e,
                    temp_folder=self.temp_folder_path,
                    func_path_modifier=convert_path_from_docker,
                )
            else:
                raise
        except ModuleNotFoundError:
            raise Exception(
                f"Please make sure the transformer for {convert_path_from_docker(self.test_path)} exists!"
            )
        finally:
            if "CATALOG_LOCATION" in os.environ:
                del os.environ["CATALOG_LOCATION"]

    def run_helix_transformers(
        self, parameters: Dict[str, Any], transformer_class: Optional[Type[Transformer]]
    ) -> None:
        # read parameters.json if it exists
        if not parameters:
            parameters = {}
        parameters_json_file: Path = Path(self.test_path).joinpath(
            self.parameters_filename
        )
        if os.path.exists(parameters_json_file):
            with open(parameters_json_file, "r") as file:
                parameters = json.loads(file.read())
                assert parameters
        # turn path into transformer name and call transformer
        # first set the view parameter since AutoMapper transformers require it
        if "view" not in parameters:
            destination_view_name: str = "output"
            parameters["view"] = destination_view_name

        with ProgressLogger() as progress_logger:
            # now figure out the class_parameters to use when instantiating the class
            class_parameters: Dict[str, Any] = {
                "parameters": parameters or {},
                "progress_logger": progress_logger,
            }
            my_instance: Transformer = ClassHelpers.instantiate_class_with_parameters(
                class_parameters=class_parameters, my_class=transformer_class  # type: ignore
            )
            # now call transform
            schema = StructType([])
            # create an empty dataframe to pass into transform()
            df: DataFrame = self.spark_session.createDataFrame(
                self.spark_session.sparkContext.emptyRDD(), schema
            )
            my_instance.transform(df)

    @staticmethod
    def find_transformer(
        testable_folder: str,
    ) -> Type[Transformer]:
        # get name of transformer file
        search_result: Optional[Match[str]] = search(r"/library/", testable_folder)
        if not search_result:
            raise ModuleNotFoundError(
                f"re.search(r`/library/`, {testable_folder}) returns empty!"
            )
        search_result_end = search_result.end()
        transformer_file_name: str = testable_folder[search_result_end:].replace(
            "/", "_"
        )
        # find parent folder of transformer file
        search_result_start_ = search_result.start() + 1
        lib_path: str = testable_folder[search_result_start_:].replace("/", ".")
        # load the transformer file (i.e., module)
        full_reference = lib_path + "." + transformer_file_name
        my_class = ClassHelpers.get_first_class_in_file(full_reference)
        return my_class
