from pathlib import Path
from typing import Optional, Union, List

from pyspark.sql.session import SparkSession
from spark_pipeline_framework_testing.mockserver_client.mockserver_client import (
    MockServerFriendlyClient,
)

from spark_pipeline_framework_testing.test_classes.input_types import FhirCalls

from spark_pipeline_framework_testing.test_classes.validator_types import (
    MockCallValidator,
)

from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore


class FhirValidator(MockCallValidator):
    """
    This class automatically reads the output tables, sends them to the fhir server to validate and then posts them to
        mock FHIR server. Then the super calls does the work of verifying them
    """

    def __init__(
        self,
        related_inputs: Optional[Union[List[FhirCalls], FhirCalls]],
    ) -> None:
        super().__init__(related_inputs=related_inputs)

    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        super().validate(
            test_name=test_name,
            test_path=test_path,
            spark_session=spark_session,
            temp_folder_path=temp_folder_path,
            logger=logger,
            mock_client=mock_client,
        )
