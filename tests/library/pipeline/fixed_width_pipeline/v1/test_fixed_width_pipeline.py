from pathlib import Path

from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.framework_fixed_width_loader.v1.framework_fixed_width_loader import (
    ColumnSpec,
)

from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)

from library.conftest import clean_spark_session
from spark_pipeline_framework_testing.test_classes import input_types


def test_fixed_width_pipeline(
    spark_session: SparkSession,
) -> None:
    clean_spark_session(session=spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_name = "test_fixed_width_pipeline"

    columns = [
        ColumnSpec(column_name="id", start_pos=1, length=3, data_type=StringType()),
        ColumnSpec(
            column_name="some_date", start_pos=4, length=8, data_type=StringType()
        ),
        ColumnSpec(
            column_name="some_string",
            start_pos=12,
            length=3,
            data_type=StringType(),
        ),
        ColumnSpec(
            column_name="some_integer",
            start_pos=15,
            length=4,
            data_type=IntegerType(),
        ),
    ]

    test_input = input_types.FileInput(fixed_width_columns=columns, row_tag="book")
    logger = get_logger(__name__)

    mock_server_url = "http://mock-server:1080"
    mock_client = MockServerFriendlyClient(mock_server_url)
    mock_client.clear(f"/{test_name}/")
    mock_client.expect_default()

    params = {
        "input_file_path": f"{data_dir.joinpath('input').joinpath('test.txt')}",
        "input_xml_path": f"{data_dir.joinpath('input').joinpath('books.xml')}",
    }

    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=data_dir,
        test_name=test_name,
        test_validators=[],
        logger=logger,
        test_inputs=[test_input],
        temp_folder="output/temp",
        mock_client=mock_client,
        helix_pipeline_parameters=params,
    ).run_test2()

    result = spark_session.table("test")
    result_xml = spark_session.table("books")
    assert result.count() == 2
    assert result.collect()[0][0] == "001"
    assert result.collect()[1][0] == "002"
    assert result.collect()[0][1] == "01292017"
    assert result.collect()[1][1] == "01302017"
    assert result.collect()[0][2] == "you"
    assert result.collect()[1][2] == "me"
    assert result.collect()[0][3] == 1234
    assert result.collect()[1][3] == 5678
    assert result_xml.count() == 12
    assert len(result_xml.columns) == 7
