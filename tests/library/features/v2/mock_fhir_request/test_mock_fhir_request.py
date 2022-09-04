import logging
from pathlib import Path

import requests
from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_classes import input_types
from spark_pipeline_framework_testing.test_classes.validator_types import (
    MockRequestValidator,
)


def test_mock_fhir_request_with_validation(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_mock_fhir_request"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(path=f"/{test_name}/*.*")

    logger = logging.getLogger(__name__)

    mock_fhir_request = input_types.MockFhirRequest(
        fhir_calls_folder="mock_requests", fhir_resource_type="InsurancePlan"
    )
    mock_fhir_request.initialize(
        test_name=test_name, test_path=test_path, mock_client=mock_client, logger=logger
    )
    requests.get(
        f"{mock_server_url}/{test_name}/4_0_0/InsurancePlan/fhir_insurance_plan_resource"
    )
    mock_call_validator = MockRequestValidator(mock_requests_folder="mock_requests")
    mock_call_validator.validate(
        test_name=test_name,
        test_path=test_path,
        spark_session=spark_session,
        temp_folder_path=test_path.joinpath("temp"),
        logger=logger,
        mock_client=mock_client,
    )


def test_mock_request_validator_without_mock_requests_folder(
    spark_session: SparkSession,
) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_mock_fhir_request"

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(path=f"/{test_name}/*.*")

    logger = logging.getLogger(__name__)

    mock_fhir_request = input_types.MockFhirRequest(
        fhir_calls_folder="mock_requests", fhir_resource_type="InsurancePlan"
    )
    mock_fhir_request.initialize(
        test_name=test_name, test_path=test_path, mock_client=mock_client, logger=logger
    )
    requests.get(
        f"{mock_server_url}/{test_name}/4_0_0/InsurancePlan/fhir_insurance_plan_resource"
    )
    mock_call_validator = MockRequestValidator()
    mock_call_validator.validate(
        test_name=test_name,
        test_path=test_path,
        spark_session=spark_session,
        temp_folder_path=test_path.joinpath("temp"),
        logger=logger,
        mock_client=mock_client,
    )
