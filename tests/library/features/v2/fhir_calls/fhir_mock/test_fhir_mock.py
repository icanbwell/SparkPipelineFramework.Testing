import json
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from requests import Response
from spark_pipeline_framework.logger.yarn_logger import get_logger

from library.pipeline.fhir_calls.fhir_mock.v1.pipeline_fhir_calls_fhir_mock_v1 import PipelineFhirCallsFhirMockV1
from spark_pipeline_framework_testing.mockserver_client.mockserver_client import MockServerFriendlyClient
from spark_pipeline_framework_testing.test_classes.input_types import FhirCalls
from spark_pipeline_framework_testing.test_classes.validator_types import MockCallValidator
from spark_pipeline_framework_testing.test_runner_v2 import SparkPipelineFrameworkTestRunnerV2


def test_fhir_mock(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_fhir_mock"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/")


    fhir_calls = FhirCalls()
    test_validators = MockCallValidator(related_inputs=fhir_calls)
    params = {
        "test_name": test_name,
        "mock_server_url": mock_server_url,
        "files_path": [
            test_path.joinpath("fhir_calls/healthcare_service/1629334859-TT3-GPPC.json"),
            test_path.joinpath("fhir_calls/healthcare_service/1790914448-TT4-GPPC.json"),
            test_path.joinpath("fhir_calls/location/Medstar-Alias-TT3-GPPC.json"),
            test_path.joinpath("fhir_calls/location/Medstar-Alias-TT4-GPPC.json"),

        ]
    }
    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(logger = logger,spark_session=spark_session, test_path=test_path, test_name=test_name,
                                       test_validators=[test_validators], auto_find_helix_transformer=False,
                                       helix_transformers=[PipelineFhirCallsFhirMockV1], mock_client=mock_client,
                                       helix_pipeline_parameters=params, test_inputs=[fhir_calls],
                                       temp_folder="temp").run_test2()
