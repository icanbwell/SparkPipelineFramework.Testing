from math import isnan
from typing import List, Any, Tuple, Optional

# noinspection PyProtectedMember
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, ArrayType, StructType, DataType

from spark_pipeline_framework.utilities.list_utils import diff_lists


def assert_compare_data_frames(
    expected_df: DataFrame,
    result_df: DataFrame,
    exclude_columns: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    snap_shot_path: Optional[str] = None
) -> None:
    if exclude_columns:
        result_df = result_df.drop(*exclude_columns)
        expected_df = expected_df.drop(*exclude_columns)
    if order_by:
        expected_df = expected_df.orderBy(*order_by)
        result_df = result_df.orderBy(*order_by)
    if include_columns:
        result_df = result_df.select(*include_columns)
        expected_df = expected_df.select(*include_columns)

    result_df = result_df.select(sorted(result_df.columns))
    expected_df = expected_df.select(sorted(expected_df.columns))

    assert sorted(result_df.columns) == sorted(
        expected_df.columns
    ), f"""Columns do not match in {snap_shot_path}.
        Columns not matched:[{diff_lists(expected_df.columns, result_df.columns)}],
        Expected:[{expected_df.columns}],
        Actual:[{result_df.columns}]"""

    print("schema for result")
    result_df.printSchema()
    print("schema for expected")
    expected_df.printSchema()
    # compare the types
    result_columns: List[Tuple[str, str]] = list(
        map(lambda t: (t.name, t.dataType.typeName()), result_df.schema)
    )
    expected_columns: List[Tuple[str, str]] = list(
        map(lambda t: (t.name, t.dataType.typeName()), expected_df.schema)
    )
    number_of_mismatched_columns: int = 0
    for i in range(0, len(result_columns)):
        result_column = result_columns[i]
        expected_column = expected_columns[i]
        if result_column != expected_column:
            print(
                f"column type for {result_column[0]} did not match in {snap_shot_path}: "
                + f"expected: {expected_column[1]}, actual: {result_column[1]}"
            )
            number_of_mismatched_columns += 1

    if number_of_mismatched_columns > 0:
        raise ValueError(
            f"{number_of_mismatched_columns} columns did not match for {snap_shot_path}.  See list above."
        )

    print("comparing result to expected")
    if expected_df.count() != result_df.count():
        print("--------- result ---------")
        result_df.show(truncate=False, n=100)
        print("--------- expected ----------")
        expected_df.show(truncate=False, n=100)
        print("------- difference (result - expected) -------")
        result_df.subtract(expected_df).show(truncate=False, n=100)
        print("------- difference (expected - result) -------")
        expected_df.subtract(result_df).show(truncate=False, n=100)
    assert expected_df.count() == result_df.count(
    ), f"expected {expected_df.count()} rows, actual {result_df.count()} rows"
    error_count: int = 0
    result_rows: List[Row] = result_df.collect()
    expected_rows: List[Row] = expected_df.collect()
    column_schemas: List[StructField] = [t for t in result_df.schema]
    for row_num in range(0, len(result_rows)):
        for column_num in range(0, len(result_columns)):
            schema_for_column: StructField = column_schemas[column_num]
            result_value = result_rows[row_num][column_num]
            expected_value = expected_rows[row_num][column_num]
            error_count = check_column_value(
                column_num=column_num,
                error_count=error_count,
                expected_value=expected_value,
                result_columns=result_columns,
                result_value=result_value,
                row_num=row_num,
                data_type_for_column=schema_for_column.dataType
            )

    if error_count > 0:
        print("--------- result ---------")
        result_df.show(truncate=False, n=100)
        print("--------- expected ----------")
        expected_df.show(truncate=False, n=100)
    assert error_count == 0, f"snapshot did not match result {snap_shot_path}.  See exact error above."


def check_column_value(
    column_num: int, error_count: int, expected_value: Any,
    result_columns: List[Tuple[str, str]], result_value: Any, row_num: int,
    data_type_for_column: DataType
) -> int:
    if isinstance(data_type_for_column, ArrayType):
        array_item: Row
        for array_item_index in range(0, len(result_value)):
            element_type: StructField = data_type_for_column.elementType
            result_array_item = result_value[array_item_index]
            expected_array_item = expected_value[array_item_index]
            if isinstance(result_array_item, Row):
                error_count = check_column_value(
                    column_num=column_num,
                    error_count=error_count,
                    expected_value=expected_array_item,
                    result_value=result_array_item,
                    result_columns=result_columns,
                    row_num=row_num,
                    data_type_for_column=element_type
                )
    elif isinstance(data_type_for_column, StructType):
        error_count = check_struct(
            column_num=column_num,
            error_count=error_count,
            expected_value=expected_value,
            result_value=result_value,
            result_columns=result_columns,
            row_num=row_num,
            data_type_for_column=data_type_for_column
        )
    else:
        error_count = check_column_simple_value(
            error_count=error_count,
            expected_value=expected_value,
            result_value=result_value,
            row_num=row_num,
            column_value=result_columns[column_num][0]
        )
    return error_count


def check_struct(
    column_num: int, error_count: int, expected_value: Row, result_value: Row,
    result_columns: List[Tuple[str, str]], row_num: int,
    data_type_for_column: StructType
) -> int:
    for struct_item_index in range(0, len(result_value)):
        result_struct_item = result_value[struct_item_index]
        expected_struct_item = expected_value[struct_item_index]
        struct_item_type: DataType = data_type_for_column.fields[
            struct_item_index].dataType
        error_count += check_column_value(
            column_num=column_num,
            error_count=error_count,
            expected_value=expected_struct_item,
            result_value=result_struct_item,
            result_columns=result_columns,
            row_num=row_num,
            data_type_for_column=struct_item_type
        )
    return error_count


def check_column_simple_value(
    error_count: int, expected_value: Any, result_value: Any, row_num: int,
    column_value: Any
) -> int:
    result_isnan = isinstance(result_value, float) and isinstance(
        expected_value, float
    ) and (isnan(result_value) == isnan(expected_value))
    # if result does not match
    #   and result is not NaN
    #   and both result and expected are not false
    if result_value != expected_value and not result_isnan and not (
        not result_value and not expected_value
    ):
        print(
            f"row {row_num}: column {column_value} " +
            f"expected: [{expected_value}] actual: [{result_value}]"
        )
        error_count += 1
    return error_count
