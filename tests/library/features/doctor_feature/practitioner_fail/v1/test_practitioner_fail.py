from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner
from spark_pipeline_framework_testing.testing_exception import SparkPipelineFrameworkTestingException


def test_practitioner_fail(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    with pytest.raises(SparkPipelineFrameworkTestingException):
        try:
            SparkPipelineFrameworkTestRunner.run_tests(
                spark_session=spark_session,
                folder_path=data_dir,
                func_path_modifier=lambda x:
                Path(str(x).replace(str(data_dir), "/foo/"))
            )
        except SparkPipelineFrameworkTestingException as e:
            assert len(e.exceptions) == 1
            assert e.exceptions[0].result_path == Path(
                "/foo/output/temp/result/output.json"
            )
            assert e.exceptions[0].expected_path == Path(
                "/foo/output/output.json"
            )
            assert e.exceptions[0].compare_path == Path(
                "/foo/output/temp/result/compare_output.json.command"
            )
            raise
