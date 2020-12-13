from typing import List

from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
)


class SparkPipelineFrameworkTestingException(Exception):
    def __init__(self, exceptions: List[SparkDataFrameComparerException]) -> None:
        self.exceptions: List[SparkDataFrameComparerException] = exceptions
        super().__init__(
            f"{len(exceptions)} Failures. {','.join(str(e) for e in exceptions)}"
        )
