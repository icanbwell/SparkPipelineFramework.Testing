import json
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from requests import Response
from spark_pipeline_framework.logger.yarn_logger import get_logger

from library.features.complex_feature.features_complex_feature import (
    FeaturesComplexFeature,
)
from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)
from spark_pipeline_framework_testing.test_classes import input_types
from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)


def test_request_response_call(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_request_response_call"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(test_name)
    input_file = input_types.FileInput()
    request1 = input_types.HttpJsonRequest(response_data_folder="request_response_call")
    request2 = input_types.HttpJsonRequest(
        response_data_folder="request_response_call2"
    )
    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=test_path,
        test_name=test_name,
        test_validators=None,
        logger=logger,
        auto_find_helix_transformer=False,
        helix_transformers=[FeaturesComplexFeature],
        mock_client=mock_client,
        test_inputs=[input_file, request1, request2],
        temp_folder="output/temp",
    ).run_test2()

    with open(test_path.joinpath("request_response_call/provider.json")) as f:
        content = json.load(f)
    with open(test_path.joinpath("request_response_call2/provider2.json")) as f:
        content2 = json.load(f)

    response: Response = requests.get(f"{mock_server_url}/{test_name}/provider?page=1")
    assert response.json() == content["request_result"]
    response = requests.get(f"{mock_server_url}/{test_name}/provider2?page=1")
    assert response.json() == content2["request_result"]
