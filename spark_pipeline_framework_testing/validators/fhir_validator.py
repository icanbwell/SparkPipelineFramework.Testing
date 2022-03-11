import json
from pathlib import Path
from typing import Optional, Union, List, Any, Dict

import requests
from furl import furl
from mockserver_client.mockserver_client import MockServerFriendlyClient
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_json, struct
from pyspark.sql.session import SparkSession
from requests import Session

from spark_pipeline_framework_testing.test_classes.input_types import (
    FhirCalls,
    FileInput,
)

from spark_pipeline_framework_testing.test_classes.validator_types import (
    MockCallValidator,
)

from spark_pipeline_framework.logger.yarn_logger import Logger  # type: ignore

from pyspark.sql.catalog import Table


class FhirValidator(MockCallValidator):
    """
    This class automatically reads the output tables, sends them to the fhir server to validate and then posts them to
        mock FHIR server. Then the super calls does the work of verifying them
    """

    def __init__(
        self,
        related_inputs: Optional[Union[List[FhirCalls], FhirCalls]],
        related_file_inputs: Union[List[FileInput], FileInput],
        mock_server_url: str,
        test_name: str,
        fhir_validation_url: Optional[str] = None,
    ) -> None:
        super().__init__(related_inputs=related_inputs)
        self.related_file_inputs = (
            related_file_inputs
            if not related_file_inputs or isinstance(related_file_inputs, list)
            else [related_file_inputs]
        )
        assert mock_server_url
        self.mock_server_url: str = mock_server_url
        assert test_name
        self.test_name: str = test_name
        self.fhir_validation_url: Optional[str] = fhir_validation_url

    def validate(
        self,
        test_name: str,
        test_path: Path,
        spark_session: SparkSession,
        temp_folder_path: Path,
        logger: Logger,
        mock_client: Optional[MockServerFriendlyClient] = None,
    ) -> None:
        assert spark_session
        assert self.related_file_inputs
        assert self.related_file_inputs[0].input_table_names  # type: ignore

        input_table_names = self.related_file_inputs[0].input_table_names  # type: ignore
        if "output" in input_table_names:
            input_table_names.remove("output")
        # write out any missing output schemas
        output_tables: List[Table] = spark_session.catalog.listTables("default")
        output_tables_for_writing_schema: List[str] = [
            t.name
            for t in output_tables
            if not t.name.startswith("expected_") and t.name not in input_table_names
        ]

        # for each output table
        for output_table in output_tables_for_writing_schema:
            # create a new table to convert the output table to json
            output_df: DataFrame = spark_session.table(output_table)
            columns: List[str] = output_df.columns
            fhir_data_frame: DataFrame = output_df.select(
                to_json(struct(*columns)).alias("fhir")
            )
            # get resourceType and then find the fhir schema of that to apply
            for row in fhir_data_frame.collect():
                # now send this row to mock fhir server as a merge command
                row_dict: Dict[str, Any] = json.loads(row["fhir"])
                if "resourceType" not in row_dict:
                    continue

                resource_type: str = row_dict["resourceType"]

                if self.fhir_validation_url:
                    # validate the resource
                    self.validate_resource(
                        fhir_validation_url=self.fhir_validation_url,
                        resource_dict=row_dict,
                        resource_type=resource_type,
                    )
                full_uri: furl = furl(self.mock_server_url)
                full_uri /= self.test_name
                full_uri /= "4_0_0"
                assert resource_type
                full_uri /= resource_type
                headers = {"Content-Type": "application/fhir+json"}
                full_uri /= "1"
                full_uri /= "$merge"
                json_payload: str = json.dumps([row_dict])
                json_payload_bytes: bytes = json_payload.encode("utf-8")
                http: Session = requests.Session()
                response = http.post(
                    url=full_uri.url, data=json_payload_bytes, headers=headers
                )
                if response and response.ok:
                    responses: List[Dict[str, Any]]
                    # check if response is json
                    response_text = response.text
                    if response_text:
                        try:
                            responses = json.loads(response_text)
                        except ValueError as e:
                            responses = [{"issue": str(e)}]
                    else:
                        responses = []
                    logger.info(f"Responses:{json.dumps(responses)}")
                else:
                    logger.error(response.status_code, response.text)
                    assert (
                        False
                    ), f"FHIR server threw an error: {response.status_code} {response.text}"

        super().validate(
            test_name=test_name,
            test_path=test_path,
            spark_session=spark_session,
            temp_folder_path=temp_folder_path,
            logger=logger,
            mock_client=mock_client,
        )

    @staticmethod
    def validate_resource(
        fhir_validation_url: str,
        resource_dict: Dict[str, Any],
        resource_type: str,
    ) -> None:
        full_uri: furl = furl(fhir_validation_url)
        assert resource_type
        full_uri /= resource_type
        headers = {"Content-Type": "application/fhir+json"}
        full_uri /= "$validate"
        json_payload: str = json.dumps(resource_dict)
        json_payload_bytes: bytes = json_payload.encode("utf-8")
        http: Session = requests.Session()
        validation_response = http.post(
            url=full_uri.url, data=json_payload_bytes, headers=headers
        )
        assert (
            validation_response.ok
        ), f"Failed validation for resource: {json_payload}: {validation_response.json()}"

        validation_json = validation_response.json()
        issues = [d for d in validation_json["issue"] if d["severity"] == "error"]
        assert (
            len(issues) == 0
        ), f"Failed validation for resource: {json_payload}: {validation_response.json()}"
