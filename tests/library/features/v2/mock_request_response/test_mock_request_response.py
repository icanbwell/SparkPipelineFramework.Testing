from pathlib import Path

from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql import SparkSession
from spark_pipeline_framework.logger.yarn_logger import get_logger

from library.pipeline.mock_request_responses.v1.pipeline_mock_request_responses_v1 import (
    PipelineMockRequestResponsesV1,
)
from spark_pipeline_framework_testing.test_classes.input_types import (
    MockRequestResponseCalls,
)
from spark_pipeline_framework_testing.test_classes.validator_types import (
    MockCallValidator,
)
from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)


def test_mock_fhir_graph_request(spark_session: SparkSession) -> None:
    """
    expect 2 $graph calls to be made to get Organization data
    """
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_mock_fhir_graph_request"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/")

    fhir_calls = MockRequestResponseCalls(
        fhir_calls_folder=f"{test_path}/mock_request_responses/Organization",
        mock_url_prefix=f"{test_name}/4_0_0/Organization",
        url_suffix="$graph",
    )
    test_validators = MockCallValidator(related_inputs=fhir_calls)
    params = {
        "test_name": test_name,
        "mock_server_url": mock_server_url,
        "url_fhir_segments": "Organization/$graph",
        "files_path": [
            test_path.joinpath("mock_request_responses/Organization/1023011178.json"),
            test_path.joinpath("mock_request_responses/Organization/1841293990.json"),
        ],
    }

    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=test_path,
        test_name=test_name,
        logger=logger,
        auto_find_helix_transformer=False,
        helix_transformers=[PipelineMockRequestResponsesV1],
        mock_client=mock_client,
        test_validators=[test_validators],
        test_inputs=[fhir_calls],
        temp_folder="temp",
        helix_pipeline_parameters=params,
    ).run_test2()


def test_mock_fhir_id_request(spark_session: SparkSession) -> None:
    """
    expect a request to return ids from server
    """
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_mock_fhir_id_request"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/")

    fhir_calls = MockRequestResponseCalls(
        fhir_calls_folder=f"{test_path}/mock_request_responses/request_ids",
        mock_url_prefix=f"{test_name}/4_0_0/Organization",
    )
    test_validators = MockCallValidator(related_inputs=fhir_calls)
    params = {
        "test_name": test_name,
        "mock_server_url": mock_server_url,
        "url_fhir_segments": "Organization",
        "files_path": [
            test_path.joinpath("mock_request_responses/request_ids/Organization.json")
        ],
    }

    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=test_path,
        test_name=test_name,
        logger=logger,
        auto_find_helix_transformer=False,
        helix_transformers=[PipelineMockRequestResponsesV1],
        mock_client=mock_client,
        test_validators=[test_validators],
        test_inputs=[fhir_calls],
        temp_folder="temp",
        helix_pipeline_parameters=params,
    ).run_test2()
