from typing import Any

import pytest
from pyspark.sql.session import SparkSession

from create_spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark_session(request: Any) -> SparkSession:
    return create_spark_session(request)
