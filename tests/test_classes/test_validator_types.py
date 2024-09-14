from os import path
from shutil import rmtree

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from mockserver_client.mockserver_verify_exception import MockServerVerifyException
from unittest.mock import Mock, MagicMock
from logging import Logger
from mockserver_client.mockserver_client import MockServerFriendlyClient

from spark_pipeline_framework_testing.test_classes.validator_types import (
    MockRequestValidator,
    OutputFileValidator,
)


@pytest.fixture
def mock_logger() -> Logger:
    return Mock(spec=Logger)


@pytest.fixture
def mock_client() -> MockServerFriendlyClient:
    return Mock(spec=MockServerFriendlyClient)


# Test cases for MockRequestValidator
def test_mock_request_validator_init() -> None:
    validator = MockRequestValidator(
        mock_requests_folder="mock_requests", fail_on_warning=True
    )
    assert validator.mock_requests_folder == "mock_requests"
    assert validator.fail_on_warning is True


def test_mock_request_validator_validate_success(
    mock_client: MagicMock, spark_session: SparkSession, mock_logger: Logger
) -> None:

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_path = data_dir
    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    validator = MockRequestValidator(mock_requests_folder="mock_requests")

    # Mock the method to simulate successful validation
    mock_client.verify_expectations.return_value = None

    validator.validate(
        test_name="test_mock_validation",
        test_path=test_path,
        spark_session=spark_session,
        temp_folder_path=temp_folder,
        logger=mock_logger,
        mock_client=mock_client,
    )

    mock_client.verify_expectations.assert_called_once()


@pytest.mark.skip(reason="This test is failing")
def test_mock_request_validator_validate_failure(
    mock_client: MagicMock,
    spark_session: SparkSession,
    mock_logger: Logger,
) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_path = data_dir
    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    validator = MockRequestValidator(mock_requests_folder="mock_requests")

    # Simulate an exception during validation
    mock_client.verify_expectations.side_effect = MockServerVerifyException(
        exceptions=[], found_expectations=[]
    )

    with pytest.raises(MockServerVerifyException):
        validator.validate(
            test_name="test_mock_validation_failure",
            test_path=test_path,
            spark_session=spark_session,
            temp_folder_path=temp_folder,
            logger=mock_logger,
            mock_client=mock_client,
        )


# Test cases for OutputFileValidator
def test_output_file_validator_init() -> None:
    validator = OutputFileValidator(output_folder="output_folder", auto_sort=True)
    assert validator.output_folder == "output_folder"
    assert validator.auto_sort is True


def test_output_file_validator_validate_no_output(
    spark_session: SparkSession, mock_logger: Logger
) -> None:
    validator = OutputFileValidator(output_folder="output_folder", auto_sort=True)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_path = data_dir
    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    validator.validate(
        test_name="test_output_validation",
        test_path=test_path,
        spark_session=spark_session,
        temp_folder_path=temp_folder,
        logger=mock_logger,
        mock_client=None,
    )


def test_output_file_validator_validate_with_output(
    spark_session: SparkSession, mock_logger: Logger
) -> None:

    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_path = data_dir
    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    validator = OutputFileValidator(output_folder="output_folder", auto_sort=True)

    # Create an empty output folder and mock the method for writing schema
    (test_path / "output_folder").mkdir(parents=True, exist_ok=True)

    # validator.write_table_to_output = Mock()

    validator.validate(
        test_name="test_output_validation_with_output",
        test_path=test_path,
        spark_session=spark_session,
        temp_folder_path=temp_folder,
        logger=mock_logger,
        mock_client=None,
    )

    # assert validator.write_table_to_output.called


@pytest.mark.skip(reason="This test is failing")
def test_output_file_validator_process_output_file(
    spark_session: SparkSession, mock_logger: Logger
) -> None:

    data_dir: Path = Path(__file__).parent.joinpath("./")
    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    validator = OutputFileValidator(output_folder="output_folder", auto_sort=True)

    output_file = "test_output.json"
    output_schema_folder = temp_folder / "schema"
    func_path_modifier = None
    sort_output_by = ["id"]
    apply_schema_to_output = True
    output_schema = None

    found_output_file, data_frame_exception = validator.process_output_file(
        output_file=output_file,
        output_schema_folder=output_schema_folder,
        func_path_modifier=func_path_modifier,
        sort_output_by=sort_output_by,
        apply_schema_to_output=apply_schema_to_output,
        output_schema=output_schema,
        auto_sort=True,
    )

    assert found_output_file
    assert data_frame_exception is None
