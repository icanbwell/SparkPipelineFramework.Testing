import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner


def cleanup_files(data_dir: Path) -> None:
    if os.path.exists(data_dir.joinpath("input_schema")):
        shutil.rmtree(data_dir.joinpath("input_schema"))

    if os.path.exists(data_dir.joinpath("output_schema")):
        shutil.rmtree(data_dir.joinpath("output_schema"))

    if os.path.exists(data_dir.joinpath("output", "complex.json")):
        os.remove(data_dir.joinpath("output", "complex.json"))


def test_folder(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    SparkPipelineFrameworkTestRunner.run_tests(
        spark_session=spark_session, folder_path=data_dir
    )

    cleanup_files(data_dir)
