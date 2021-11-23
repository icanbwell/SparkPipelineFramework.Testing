import json
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests import Response
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FhirRequestReponses(FrameworkPipeline):
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger, run_id: str
    ):
        test_name = parameters["test_name"]
        mock_server_url = parameters["mock_server_url"]
        resources: List[Path] = parameters["files_path"]
        url_fhir_segments: str = parameters["url_fhir_segments"]
        super().__init__(parameters=parameters, progress_logger=progress_logger)

        for fhir_file in resources:
            with open(fhir_file) as f:
                content: Dict[str, Any] = json.load(f)
                request_parameters = content["request_parameters"]
                querystring_parameters: Dict[str, str] = request_parameters[
                    "querystring"
                ]
                url = f"{mock_server_url}/{test_name}/4_0_0/{url_fhir_segments}"
                if request_parameters["method"] == "POST":
                    request_body = request_parameters["body"]["json"]
                    response: Response = requests.post(
                        f"{url}", json=request_body, params=querystring_parameters
                    )
                else:
                    response = requests.get(f"{url}", params=querystring_parameters)
                print(">>>", response.text)

        self.steps = []
