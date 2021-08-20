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


def test_source_api_call(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_source_api_call"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(test_name)

    input_file = input_types.FileInput()
    request = input_types.HttpJsonRequest()
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
        test_inputs=[input_file, request],
        temp_folder="output/temp",
    ).run_test2()

    with open(test_path.joinpath("source_api_calls/getProviderApptTypes.json")) as f:
        content = json.load(f)

    response: Response = requests.get(
        f"{mock_server_url}/{test_name}/getProviderApptTypes.json"
    )
    assert response.json() == content
