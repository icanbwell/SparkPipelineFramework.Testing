from pathlib import Path

import pytest
from spark_pipeline_framework.logger.yarn_logger import get_logger

from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)

from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)

from spark_pipeline_framework_testing.test_classes import input_types
from spark_pipeline_framework_testing.validators.fhir_validator import FhirValidator


def test_doctor_feature_practitioner_fail_on_fhir_validation(
    spark_session: SparkSession,
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_doctor_feature_practitioner_fail_on_fhir_validation"
    test_input = input_types.FileInput()
    test_fhir = input_types.FhirCalls()

    logger = get_logger(__name__)

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/")
    mock_client.expect_default()

    params = {
        "test_name": test_name,
        "mock_server_url": mock_server_url,
    }

    test_validator = FhirValidator(
        related_inputs=test_fhir,
        related_file_inputs=test_input,
        mock_server_url=mock_server_url,
        test_name=test_name,
        fhir_validation_url="http://fhir:3000/4_0_0",
    )

    with pytest.raises(AssertionError, match=r"Failed validation for resource*"):
        SparkPipelineFrameworkTestRunnerV2(
            spark_session=spark_session,
            test_path=data_dir,
            test_name=test_name,
            test_validators=[
                test_validator,
                # OutputFileValidator(related_inputs=test_input, sort_output_by=["id"]),
            ],
            logger=logger,
            test_inputs=[test_input],
            temp_folder="output/temp",
            mock_client=mock_client,
            helix_pipeline_parameters=params,
        ).run_test2()
