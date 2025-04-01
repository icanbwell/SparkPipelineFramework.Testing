from typing import Any, Generator

import pytest
from pyspark.sql.session import SparkSession

from create_spark_session import create_spark_session
from spark_pipeline_framework_testing.register import register


@pytest.fixture(scope="session")
def spark_session(request: Any) -> SparkSession:
    return create_spark_session(request)


@pytest.fixture(scope="session", autouse=True)
def run_before_each_test() -> Generator[None, Any, None]:
    # This code will run before every test
    # print("Setting up something before each test")
    # You can do setup operations here
    # For example, initializing databases, clearing caches, etc.
    print("Setting up before each test")

    register()

    # Optional: You can yield if you want to do tear down after the test
    yield

    # Optional teardown code here
    print("Cleaning up after each test")
