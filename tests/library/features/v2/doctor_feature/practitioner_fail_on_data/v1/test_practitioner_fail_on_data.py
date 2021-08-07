from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from library.features.v1.doctor_feature.practitioner_fail_on_data.v1.features_doctor_feature_practitioner_fail_on_data_v1 import (
    FeaturesDoctorFeaturePractitionerFailOnDataV1,
)
from spark_pipeline_framework_testing.test_classes.input_types import FileInput
from spark_pipeline_framework_testing.test_classes.validator_types import (
    OutputFileValidator,
)
from spark_pipeline_framework_testing.test_runner_v2 import (
    SparkPipelineFrameworkTestRunnerV2,
)
from spark_pipeline_framework_testing.testing_exception import (
    SparkPipelineFrameworkTestingException,
)


def test_practitioner_fail_on_data(spark_session: SparkSession) -> None:

    with pytest.raises(SparkPipelineFrameworkTestingException):
        try:
            test_path: Path = Path(__file__).parent.joinpath("./")

            test_name = "complex test 2"
            input_file = FileInput()
            SparkPipelineFrameworkTestRunnerV2(spark_session=spark_session, test_path=test_path, test_name=test_name,
                                               helix_transformers=[FeaturesDoctorFeaturePractitionerFailOnDataV1],
                                               test_validators=[
                                                   OutputFileValidator(
                                                       related_input=input_file,
                                                       temp_folder="output/temp",
                                                       func_path_modifier=lambda x: Path(
                                                           str(x).replace(str(test_path), "/foo/")
                                                       ),
                                                   )
                                               ], mock_client=None, test_inputs=[input_file], temp_folder="output/temp",
                                               capture_exceptions=False).run_test2()
        except SparkPipelineFrameworkTestingException as e:
            assert len(e.exceptions) == 1
            assert e.exceptions[0].result_path == Path("/foo/output/temp/output.json")
            assert e.exceptions[0].expected_path == Path("/foo/output/output.json")
            assert e.exceptions[0].compare_path == Path(
                "/foo/output/temp/compare_output.json.command"
            )
            raise
