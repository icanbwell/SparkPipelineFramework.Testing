import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from spark_pipeline_framework.logger.yarn_logger import get_logger

from library.features.complex_feature.features_complex_feature import (
    FeaturesComplexFeature,
)
from spark_pipeline_framework_testing.test_classes.input_types import FileInput
from spark_pipeline_framework_testing.test_classes.validator_types import (
    OutputFileValidator,
)
from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)


def cleanup_files(data_dir: Path) -> None:
    if os.path.exists(data_dir.joinpath("input_schema")):
        shutil.rmtree(data_dir.joinpath("input_schema"))

    if os.path.exists(data_dir.joinpath("output_schema")):
        shutil.rmtree(data_dir.joinpath("output_schema"))

    if os.path.exists(data_dir.joinpath("output", "complex.json")):
        os.remove(data_dir.joinpath("output", "complex.json"))


def test_complex_feature(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")

    test_name = "test_complex_feature"
    input_file = FileInput()
    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=test_path,
        test_name=test_name,
        test_validators=[OutputFileValidator(related_inputs=input_file)],
        logger=logger,
        auto_find_helix_transformer=False,
        helix_transformers=[FeaturesComplexFeature],
        test_inputs=[input_file],
        temp_folder="output/temp",
    ).run_test2()

    cleanup_files(test_path)
