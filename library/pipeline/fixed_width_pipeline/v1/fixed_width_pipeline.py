from typing import Dict, Any

from pyspark.sql.types import StringType, IntegerType
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_fixed_width_loader.v1.framework_fixed_width_loader import (
    FrameworkFixedWidthLoader,
    ColumnSpec,
)
from spark_pipeline_framework.transformers.framework_xml_loader.v1.framework_xml_loader import (
    FrameworkXmlLoader,
)


class FixedWidthPipeline(FrameworkPipeline):
    def __init__(
        self, parameters: Dict[str, Any], progress_logger: ProgressLogger, run_id: str
    ):
        super().__init__(parameters=parameters, progress_logger=progress_logger)
        self.steps = [
            FrameworkFixedWidthLoader(
                view="my_view",
                filepath=parameters["input_file_path"],
                columns=[
                    ColumnSpec(
                        column_name="id", start_pos=1, length=3, data_type=StringType()
                    ),
                    ColumnSpec(
                        column_name="some_date",
                        start_pos=4,
                        length=8,
                        data_type=StringType(),
                    ),
                    ColumnSpec(
                        column_name="some_string",
                        start_pos=12,
                        length=3,
                        data_type=StringType(),
                    ),
                    ColumnSpec(
                        column_name="some_integer",
                        start_pos=15,
                        length=4,
                        data_type=IntegerType(),
                    ),
                ],
            ),
            FrameworkXmlLoader(
                view="my_xml_view",
                filepath=parameters["input_xml_path"],
                row_tag="book",
            ),
        ]
