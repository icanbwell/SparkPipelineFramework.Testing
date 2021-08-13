from pathlib import Path

from pyspark.sql import SparkSession
from spark_pipeline_framework.logger.yarn_logger import get_logger

from library.features.people.my_people_feature.features_people_my_people_feature import (
    FeaturesPeopleMyPeopleFeature,
)
from spark_pipeline_framework_testing.test_classes.input_types import FileInput
from spark_pipeline_framework_testing.test_classes.validator_types import (
    OutputFileValidator,
)
from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)


def test_folder(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")

    test_name = "test_folder"
    input_file = FileInput()
    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=test_path,
        test_name=test_name,
        test_validators=[OutputFileValidator(related_inputs=input_file)],
        logger=logger,
        auto_find_helix_transformer=False,
        helix_transformers=[FeaturesPeopleMyPeopleFeature],
        test_inputs=[input_file],
        temp_folder="output/temp",
        parameters_filename="hello.json",
    ).run_test2()
