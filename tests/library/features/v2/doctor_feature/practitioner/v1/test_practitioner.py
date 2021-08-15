from pathlib import Path

from spark_pipeline_framework.logger.yarn_logger import get_logger

from library.features.doctor_feature.practitioner.v1.features_doctor_feature_practitioner_v1 import (
    FeaturesDoctorFeaturePractitionerV1,
)
from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_classes.input_types import FileInput
from spark_pipeline_framework_testing.test_classes.validator_types import (
    OutputFileValidator,
)
from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)


def test_practitioner(spark_session: SparkSession) -> None:
    test_path: Path = Path(__file__).parent.joinpath("./")

    # setup servers
    test_name = "practitioner"

    input_file = FileInput()

    logger = get_logger(__name__)
    logger = get_logger(__name__)
    SparkPipelineFrameworkTestRunnerV2(
        spark_session=spark_session,
        test_path=test_path,
        test_name=test_name,
        test_validators=[
            OutputFileValidator(related_inputs=input_file, sort_output_by=["id"])
        ],
        logger=logger,
        auto_find_helix_transformer=False,
        helix_transformers=[FeaturesDoctorFeaturePractitionerV1],
        test_inputs=[input_file],
        temp_folder="output/temp",
    ).run_test2()
