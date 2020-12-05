from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from spark_data_frame_comparer.spark_data_frame_comparer_exception import SparkDataFrameComparerException

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner


def test_folder(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    with pytest.raises(SparkDataFrameComparerException):
        SparkPipelineFrameworkTestRunner.run_tests(
            spark_session=spark_session, folder_path=data_dir
        )
