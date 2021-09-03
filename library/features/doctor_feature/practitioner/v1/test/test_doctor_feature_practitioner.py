from pathlib import Path

from spark_pipeline_framework.logger.yarn_logger import get_logger

from pyspark.sql import SparkSession
from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)

from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)

from spark_pipeline_framework_testing.test_classes import input_types, validator_types


def test_doctor_feature_practitioner(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_doctor_feature_practitioner"
    test_input = input_types.FileInput()
    test_fhir = input_types.FhirCalls()
    test_validator = validator_types.MockCallValidator(related_inputs=test_fhir)

    logger = get_logger(__name__)

    mock_client_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_client_url)
    mock_client.clear(test_name)

    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=data_dir,
        test_name=test_name,
        test_validators=[test_validator],
        logger=logger,
        test_inputs=[test_input],
        temp_folder="output/temp",
        mock_client=mock_client,
    ).run_test2()
