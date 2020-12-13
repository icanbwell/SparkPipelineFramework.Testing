from os import path
from pathlib import Path
from shutil import rmtree

import pytest
from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_runner import (
    SparkPipelineFrameworkTestRunner,
)
from spark_pipeline_framework_testing.testing_exception import (
    SparkPipelineFrameworkTestingException,
)


def test_practitioner_fail_on_schema(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SparkPipelineFrameworkTestingException):
        try:
            SparkPipelineFrameworkTestRunner.run_tests(
                spark_session=spark_session,
                folder_path=data_dir,
                temp_folder=temp_folder,
                func_path_modifier=lambda x: Path(
                    str(x).replace(str(data_dir), "/foo/")
                ),
            )
        except SparkPipelineFrameworkTestingException as e:
            assert len(e.exceptions) == 1
            assert e.exceptions[0].result_path == Path("/foo/temp/result/output.json")
            assert e.exceptions[0].expected_path == Path("/foo/output/output.json")
            assert e.exceptions[0].compare_path == Path(
                "/foo/temp/result/compare_schema_output.json.command"
            )
            compare_file_full_path = data_dir.joinpath(
                "temp/result/compare_schema_output.json.command"
            )
            with open(compare_file_full_path, "r") as file:
                print(f"------- compare file: {compare_file_full_path} ---------")
                print(file.read())
            raise
