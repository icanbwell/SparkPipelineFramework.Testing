from pathlib import Path

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner


def test_all() -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    SparkPipelineFrameworkTestRunner.run_tests(data_dir)
