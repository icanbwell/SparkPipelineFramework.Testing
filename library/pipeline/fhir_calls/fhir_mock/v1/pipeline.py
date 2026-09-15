import json
import re
from pathlib import Path
from typing import Any, Dict, List, Union
from urllib.parse import urlparse, urlunparse

import requests
from requests import Response
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

# Seconds to wait for the mock FHIR server. These calls previously passed no
# timeout, so an unresponsive mock server hung the test suite indefinitely
# instead of failing it.
MOCK_SERVER_TIMEOUT_SECONDS = 30


def build_validated_url(
    base_url: str,
    test_name: str,
    resource_name: str,
) -> str:
    try:
        # Minimal path validation
        if "/../" in base_url or re.search(r"/%2e%2e/", base_url, re.IGNORECASE):
            raise ValueError("Invalid path")

        parsed = urlparse(base_url)

        # Protocol + host checks
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Invalid protocol")
        if not parsed.hostname:
            raise ValueError("Invalid host")
        allowed_domains = ["example.com"]  # add your allowed domains here
        if parsed.hostname.lower() not in allowed_domains:
            raise ValueError("Invalid host")

        # Validate path parameters
        if not re.fullmatch(r"[A-Za-z0-9_-]+", test_name):
            raise ValueError("Invalid parameter")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", resource_name):
            raise ValueError("Invalid parameter")

        # Rebuild path from fixed literals + validated segments
        parsed = parsed._replace(path=f"/{test_name}/4_0_0/{resource_name}/1/$merge")

        return urlunparse(parsed)
    except Exception:
        raise ValueError("Invalid URL")


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
                    url = build_validated_url(mock_server_url, test_name, resource_name)
                    response: Response = requests.post(
                        url, json=resource, timeout=MOCK_SERVER_TIMEOUT_SECONDS
                    )
                    assert response.ok
                    print(">>>", response.text)
            elif isinstance(content, dict):
                resource_name = content.get("resourceType")
                url = build_validated_url(mock_server_url, test_name, resource_name)
                response = requests.post(
                    url, json=[content], timeout=MOCK_SERVER_TIMEOUT_SECONDS
                )
                assert response.ok
                print(">>>", response.text)

        self.steps = []
