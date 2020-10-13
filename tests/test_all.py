from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner


def test_all(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    SparkPipelineFrameworkTestRunner.run_tests(spark_session=spark_session, folder_path=data_dir)

    # Assert
    result_df: DataFrame = spark_session.table("patient")
    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.count() == 2

    result_df = spark_session.table("diagnosis")
    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.count() == 3
