import json
from pathlib import Path
from typing import Any, Dict, List, Union

import requests
from requests import Response
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


class FhirCalls(FrameworkPipeline):
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger, run_id: str
    ):
        test_name = parameters["test_name"]
        mock_server_url = parameters["mock_server_url"]
        resources: List[Path] = parameters["files_path"]
        super().__init__(parameters=parameters, progress_logger=progress_logger)

        for fhir_file in resources:
            with open(fhir_file) as f:
                content: Union[Dict[str, Any], List[Dict[str, Any]]] = json.load(f)
            if isinstance(content, list):
                for resource in content:
                    resource_name = resource.get("resourceType")
                    url = (
                        f"{mock_server_url}/{test_name}/4_0_0/{resource_name}/1/$merge"
                    )
                    response: Response = requests.post(f"{url}", json=resource)
                    assert response.ok
                    print(">>>", response.text)
            elif isinstance(content, dict):
                resource_name = content.get("resourceType")
                url = f"{mock_server_url}/{test_name}/4_0_0/{resource_name}/1/$merge"
                response = requests.post(f"{url}", json=[content])
                assert response.ok
                print(">>>", response.text)

        self.steps = []
