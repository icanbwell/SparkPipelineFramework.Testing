from pathlib import Path

from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_runner import (
    SparkPipelineFrameworkTestRunner,
)


def test_json_feature(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    SparkPipelineFrameworkTestRunner.run_tests(
        spark_session=spark_session, folder_path=data_dir
    )
