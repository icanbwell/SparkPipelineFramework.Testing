import json
import os
from importlib import import_module
from inspect import signature
from os import listdir
from os.path import isfile, join
from pathlib import Path, PurePath
from re import search
from typing import List, Optional, Match, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.spark_data_frame_comparer import assert_compare_data_frames


class SparkPipelineFrameworkTestRunner:
    @staticmethod
    def run_tests(spark_session: SparkSession, folder_path: Path, parameters: Dict[str, Any] = {}) -> None:
        # iterate through sub_folders trying to find folders that contain input and output folders
        testable_folder_list: List[str] = testable_folders(folder_path=folder_path)
        print(testable_folder_list)
        # for each of them
        for testable_folder in testable_folder_list:
            # read the input files
            input_folder: Path = Path(testable_folder).joinpath("input")
            input_files: List[str] = [f for f in listdir(input_folder) if isfile(join(input_folder, f))]
            input_schema_folder = Path(testable_folder).joinpath("input_schema")

            # first clear any stuff in SparkSession
            clean_spark_session(session=spark_session)

            print(f"Running test in folder: {testable_folder}...")

            # for each file in input folder, load into a view in Spark
            #   (use name of file without extension as name of view)
            for input_file in input_files:
                file_extension: str
                _, file_extension = os.path.splitext(input_file)
                filename: str
                filename, _ = os.path.splitext(PurePath(input_file).name)
                if file_extension.lower() == ".csv":
                    input_file_path = os.path.join(input_folder, input_file)
                    input_schema_file = os.path.join(input_schema_folder, f"{filename}.json")
                    if os.path.exists(input_schema_file):
                        with open(input_schema_file) as file:
                            schema_json: str = json.loads(file.read())
                        schema = StructType.fromJson(schema_json)
                        print(f"Reading file {input_file} using schema: {input_schema_file}")
                        spark_session.read.schema(schema).csv(
                            path=input_file_path,
                            header=True).createOrReplaceTempView(filename)
                    else:
                        spark_session.read.csv(
                            path=input_file_path,
                            header=True).createOrReplaceTempView(filename)
                elif file_extension.lower() == ".jsonl" or file_extension.lower() == ".json":
                    input_file_path = os.path.join(input_folder, input_file)
                    input_schema_file = os.path.join(input_schema_folder, input_file)
                    if os.path.exists(input_schema_file):
                        with open(input_schema_file) as file:
                            schema_json = json.loads(file.read())
                        schema = StructType.fromJson(schema_json)
                        print(f"Reading file {input_file} using schema: {input_schema_file}")
                        spark_session.read.schema(schema).json(
                            path=input_file_path
                        ).createOrReplaceTempView(filename)
                    else:
                        spark_session.read.json(
                            path=input_file_path
                        ).createOrReplaceTempView(filename)

                elif file_extension.lower() == ".parquet":
                    spark_session.read.parquet(
                        path=os.path.join(input_folder, input_file)
                    ).createOrReplaceTempView(filename)

            # turn path into transformer name
            # call transformer

            # set the view parameter
            if "view" not in parameters:
                output_folder: Path = Path(testable_folder).joinpath("output")
                output_files: List[str] = [f for f in listdir(output_folder) if
                                           isfile(join(output_folder, f)) and not f.endswith(".py")]
                destination_view_name: Optional[str] = os.path.splitext(PurePath(output_files[0]).name)[0] if len(
                    output_files) > 0 else "output"
                parameters["view"] = destination_view_name

            # find name of transformer
            search_result: Optional[Match[str]] = search(r'/library/', testable_folder)
            if search_result:
                transformer_file_name = testable_folder[search_result.end():].replace('/', '_')
                lib_path = testable_folder[search_result.start() + 1:].replace('/', '.')
                module = import_module(lib_path + "." + transformer_file_name)
                md = module.__dict__
                my_class = [md[c] for c in md if (isinstance(md[c], type) and md[c].__module__ == module.__name__)][0]
                my_class_signature = signature(my_class.__init__)
                my_class_args = [param.name for param in my_class_signature.parameters.values() if param.name != 'self']
                with ProgressLogger() as progress_logger:
                    # now figure out the class_parameters to use when instantiating the class
                    class_parameters: Dict[str, Any] = {
                        "parameters": parameters,
                        'progress_logger': progress_logger
                    }

                    if len(my_class_args) > 0 and len(class_parameters) > 0:
                        my_instance = my_class(**{k: v for k, v in class_parameters.items() if k in my_class_args})
                    else:
                        my_instance = my_class()
                    # now call transform
                    schema = StructType([])

                    df: DataFrame = spark_session.createDataFrame(
                        spark_session.sparkContext.emptyRDD(), schema)

                    my_instance.transformers[0].transform(df)

            # for each file in output folder, loading into a view in Spark (prepend with "expected_")
            output_folder = Path(testable_folder).joinpath("output")
            output_files = [f for f in listdir(output_folder) if isfile(join(output_folder, f))]
            output_schema_folder = Path(testable_folder).joinpath("output_schema")
            for output_file in output_files:
                _, file_extension = os.path.splitext(output_file)
                filename, _ = os.path.splitext(PurePath(output_file).name)
                found_output_file: bool = False
                output_file_path = os.path.join(output_folder, output_file)
                if file_extension.lower() == ".csv":
                    output_schema_file = os.path.join(output_schema_folder, f"{filename}.json")
                    if os.path.exists(output_schema_file):
                        with open(output_schema_file) as file:
                            schema_json = json.loads(file.read())
                        schema = StructType.fromJson(schema_json)
                        print(f"Reading file {output_file_path} using schema: {output_schema_file}")
                        spark_session.read.schema(schema).csv(
                            path=output_file_path,
                            header=True
                        ).createOrReplaceTempView(f"expected_{filename}")
                    else:
                        spark_session.read.csv(
                            path=output_file_path,
                            header=True
                        ).createOrReplaceTempView(f"expected_{filename}")

                    found_output_file = True
                elif file_extension.lower() == ".jsonl" or file_extension.lower() == ".json":
                    output_schema_file = os.path.join(output_schema_folder, output_file)
                    if os.path.exists(output_schema_file):
                        with open(output_schema_file) as file:
                            schema_json = json.loads(file.read())
                        schema = StructType.fromJson(schema_json)
                        print(f"Reading file {output_file_path} using schema: {output_schema_file}")
                        spark_session.read.schema(schema).json(
                            path=output_file_path
                        ).createOrReplaceTempView(f"expected_{filename}")
                    else:
                        spark_session.read.json(
                            path=output_file_path
                        ).createOrReplaceTempView(f"expected_{filename}")
                    found_output_file = True
                elif file_extension.lower() == ".parquet":
                    spark_session.read.parquet(
                        path=output_file_path
                    ).createOrReplaceTempView(f"expected_{filename}")
                    found_output_file = True

                if found_output_file:
                    # Do a data frame compare on each view
                    print(f"Comparing with view:[filename= with view:[expected_{filename}]")
                    # drop any corrupted column
                    assert_compare_data_frames(
                        expected_df=spark_session.table(f"expected_{filename}").drop("_corrupt_record"),
                        result_df=spark_session.table(filename)
                    )


def testable_folders(folder_path: Path) -> List[str]:
    folder_list: List[str] = list_folders(folder_path=folder_path)
    testable_folder_list: List[str] = [
        str(PurePath(folder_path).parent)
        for folder_path in folder_list
        if PurePath(folder_path).name == "input"
    ]
    return testable_folder_list


def list_files(folder_path: Path) -> List[str]:
    file_list = []
    for root, dirs, files in os.walk(top=folder_path):
        for name in files:
            file_list.append(os.path.join(root, name))
    return file_list


def list_folders(folder_path: Path) -> List[str]:
    folder_list = []
    for root, dirs, files in os.walk(top=folder_path):
        folder_list.append(root)
    return folder_list


def clean_spark_session(session: SparkSession) -> None:
    """

    :param session:
    :return:
    """
    tables = session.catalog.listTables("default")

    for table in tables:
        print(f"clear_tables() is dropping table/view: {table.name}")
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        session.sql(f"DROP TABLE IF EXISTS default.{table.name}")
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        session.sql(f"DROP VIEW IF EXISTS default.{table.name}")
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        session.sql(f"DROP VIEW IF EXISTS {table.name}")

    session.catalog.clearCache()
