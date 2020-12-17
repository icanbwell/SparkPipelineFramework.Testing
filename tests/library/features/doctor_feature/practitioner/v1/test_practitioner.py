from os import path
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_runner import (
    SparkPipelineFrameworkTestRunner,
)


def test_practitioner(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("output/temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    SparkPipelineFrameworkTestRunner.run_tests(
        spark_session=spark_session, folder_path=data_dir, sort_output_by=["id"]
    )
