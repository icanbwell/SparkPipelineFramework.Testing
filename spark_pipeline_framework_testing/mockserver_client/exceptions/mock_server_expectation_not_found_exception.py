from spark_pipeline_framework_testing.mockserver_client.exceptions.mock_server_exception import (
    MockServerException,
)


class MockServerExpectationNotFoundException(MockServerException):
    def __init__(self, url: str, json: str) -> None:
        self.url: str = url
        self.json: str = json
        super().__init__(f"Expectation not met: {url} {json}")
