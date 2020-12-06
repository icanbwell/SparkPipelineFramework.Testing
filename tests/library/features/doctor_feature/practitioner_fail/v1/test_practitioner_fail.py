from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from spark_data_frame_comparer.spark_data_frame_comparer_exception import SparkDataFrameComparerException

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner


def test_practitioner_fail(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    with pytest.raises(SparkDataFrameComparerException):
        try:
            SparkPipelineFrameworkTestRunner.run_tests(
                spark_session=spark_session,
                folder_path=data_dir,
                func_path_modifier=lambda x:
                Path(str(x).replace(str(data_dir), "/foo/"))
            )
        except SparkDataFrameComparerException as e:
            assert e.result_path == Path("/foo/output/temp/result/output.json")
            assert e.expected_path == Path("/foo/output/output.json")
            assert e.compare_path == Path(
                "/foo/output/temp/result/compare_output.json.command"
            )
            raise
