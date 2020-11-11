import json
import os
import glob
import shutil
from importlib import import_module
from inspect import signature
from os import listdir
from os.path import isfile, join
from pathlib import Path, PurePath
from re import search
from shutil import copyfile
from typing import List, Optional, Match, Dict, Any, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Table
from pyspark.sql.types import StructType
from spark_data_frame_comparer.spark_data_frame_comparer import assert_compare_data_frames
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.json_to_jsonl_converter import convert_json_to_jsonl


class SparkPipelineFrameworkTestRunner:
    row_limit: int = 100

    @staticmethod
    def run_tests(
        spark_session: SparkSession,
        folder_path: Path,
        parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        if not parameters:
            parameters = {}

        # iterate through sub_folders trying to find folders that contain input and output folders
        testable_folder_list: List[str] = get_testable_folders(
            folder_path=folder_path
        )

        # first clear any stuff in SparkSession
        clean_spark_session(session=spark_session)

        # for each of them
        testable_folder: str
        for testable_folder in testable_folder_list:
            input_folder: Path = Path(testable_folder).joinpath("input")
            input_files: List[str] = [
                f for f in listdir(input_folder)
                if isfile(join(input_folder, f))
            ]
            input_schema_folder = Path(testable_folder
                                       ).joinpath("input_schema")

            print(f"Running test in folder: {testable_folder}...")

            # for each file in input folder, load into a view in Spark
            #   (use name of file without extension as name of view)
            for input_file in input_files:
                SparkPipelineFrameworkTestRunner.process_input_file(
                    spark_session=spark_session,
                    input_file=input_file,
                    input_folder=input_folder,
                    input_schema_folder=input_schema_folder
                )

            # write out any input schemas
            input_tables: List[Table] = spark_session.catalog.listTables(
                "default"
            )

            input_table_names: List[str] = [
                t.name for t in input_tables
                if not t.name.startswith("expected_")
            ]
            table_name: str
            for table_name in input_table_names:
                if not os.path.exists(input_schema_folder):
                    os.mkdir(input_schema_folder)

                SparkPipelineFrameworkTestRunner.write_schema_to_output(
                    spark_session=spark_session,
                    view_name=table_name,
                    schema_folder=input_schema_folder
                )

            # read parameters.json if it exists
            parameters_json_file: Path = Path(testable_folder
                                              ).joinpath("parameters.json")
            if os.path.exists(parameters_json_file):
                with open(parameters_json_file, "r") as file:
                    parameters = json.loads(file.read())
                    assert parameters

            # turn path into transformer name and call transformer
            # first set the view parameter since AutoMapper transformers require it
            if "view" not in parameters:
                destination_view_name: str = "output"
                parameters["view"] = destination_view_name

            # find name of transformer
            search_result: Optional[Match[str]
                                    ] = search(r'/library/', testable_folder)
            if search_result:
                SparkPipelineFrameworkTestRunner.run_transformer(
                    spark_session=spark_session,
                    parameters=parameters,
                    search_result=search_result,
                    testable_folder=testable_folder
                )

            # write out any missing schemas
            output_tables: List[Table] = spark_session.catalog.listTables(
                "default"
            )

            output_schema_folder: Path = Path(testable_folder
                                              ).joinpath("output_schema")
            tables_for_writing_schema: List[str] = [
                t.name for t in output_tables
                if not t.name.startswith("expected_")
                and t.name not in input_table_names
            ]
            if "output" in tables_for_writing_schema:  # if there is an output table then ignore other input_tables
                tables_for_writing_schema = ["output"]
            for table_name in tables_for_writing_schema:
                if not os.path.exists(output_schema_folder):
                    os.mkdir(output_schema_folder)

                SparkPipelineFrameworkTestRunner.write_schema_to_output(
                    spark_session=spark_session,
                    view_name=table_name,
                    schema_folder=output_schema_folder
                )

            # for each file in output folder, loading into a view in Spark (prepend with "expected_")
            output_folder = Path(testable_folder).joinpath("output")
            if not os.path.exists(output_folder):
                os.mkdir(output_folder)
            output_files = [
                f for f in listdir(output_folder)
                if isfile(join(output_folder, f))
            ]
            views_found: List[str] = []
            for output_file in output_files:

                found_output_file: bool = SparkPipelineFrameworkTestRunner.process_output_file(
                    spark_session=spark_session,
                    output_file=output_file,
                    output_folder=output_folder,
                    output_schema_folder=output_schema_folder
                )
                if found_output_file:
                    views_found.append(
                        SparkPipelineFrameworkTestRunner.
                        get_view_name_from_file_path(output_file).lower()
                    )

            # write out any missing output files
            if os.path.exists(output_folder.joinpath("temp")):
                shutil.rmtree(output_folder.joinpath("temp"))

            table_names_to_write_to_output: List[str] = [
                t.name for t in output_tables
                if t.name.lower() not in views_found and not t.name.
                startswith("expected_") and t.name not in input_table_names
            ]
            if "output" in table_names_to_write_to_output:  # if there is an output table then ignore other input_tables
                table_names_to_write_to_output = ["output"]
            for table_name in table_names_to_write_to_output:
                SparkPipelineFrameworkTestRunner.write_table_to_output(
                    spark_session=spark_session,
                    view_name=table_name,
                    output_folder=output_folder
                )
            if os.path.exists(output_folder.joinpath("temp")):
                shutil.rmtree(output_folder.joinpath("temp"))

            clean_spark_session(session=spark_session)

    @staticmethod
    def run_transformer(
        spark_session: SparkSession, parameters: Optional[Dict[str, Any]],
        search_result: Match[str], testable_folder: str
    ) -> None:
        # get name of transformer file
        transformer_file_name = testable_folder[search_result.end():].replace(
            '/', '_'
        )
        # find parent folder of transformer file
        lib_path = testable_folder[search_result.start() +
                                   1:].replace('/', '.')
        # load the transformer file (i.e., module)
        module = import_module(lib_path + "." + transformer_file_name)
        md = module.__dict__
        # find the first class in that module (we assume the first class is the Transformer class)
        my_class = [
            md[c] for c in md if
            (isinstance(md[c], type) and md[c].__module__ == module.__name__)
        ][0]
        # find the signature of the __init__ method
        my_class_signature = signature(my_class.__init__)
        my_class_args = [
            param.name for param in my_class_signature.parameters.values()
            if param.name != 'self'
        ]
        with ProgressLogger() as progress_logger:
            # now figure out the class_parameters to use when instantiating the class
            class_parameters: Dict[str, Any] = {
                "parameters": parameters or {},
                'progress_logger': progress_logger
            }
            # instantiate the class passing in the parameters + progress_logger
            if len(my_class_args) > 0 and len(class_parameters) > 0:
                my_instance = my_class(
                    **{
                        k: v
                        for k, v in class_parameters.items()
                        if k in my_class_args
                    }
                )
            else:
                my_instance = my_class()
            # now call transform
            schema = StructType([])
            # create an empty dataframe to pass into transform()
            df: DataFrame = spark_session.createDataFrame(
                spark_session.sparkContext.emptyRDD(), schema
            )

            my_instance.transformers[0].transform(df)

    @staticmethod
    def process_output_file(
        spark_session: SparkSession, output_file: str, output_folder: Path,
        output_schema_folder: Path
    ) -> bool:
        file_extension: str = SparkPipelineFrameworkTestRunner.get_file_extension_from_file_path(
            output_file
        )
        view_name: str = SparkPipelineFrameworkTestRunner.get_view_name_from_file_path(
            output_file
        )
        found_output_file: bool = False
        output_file_path = os.path.join(output_folder, output_file)
        if file_extension.lower() == ".csv":
            output_schema_file = os.path.join(
                output_schema_folder, f"{view_name}.json"
            )
            if os.path.exists(output_schema_file):
                with open(output_schema_file) as file:
                    schema_json = json.loads(file.read())
                schema = StructType.fromJson(schema_json)
                print(
                    f"Reading file {output_file_path} using schema: {output_schema_file}"
                )
                spark_session.read.schema(schema).csv(
                    path=output_file_path, header=True
                ).createOrReplaceTempView(f"expected_{view_name}")
            else:
                spark_session.read.csv(
                    path=output_file_path, header=True, comment="#"
                ).createOrReplaceTempView(f"expected_{view_name}")

            found_output_file = True
        elif file_extension.lower() == ".jsonl" or file_extension.lower(
        ) == ".json":
            output_schema_file = os.path.join(
                output_schema_folder, output_file
            )
            if os.path.exists(output_schema_file):
                with open(output_schema_file) as file:
                    schema_json = json.loads(file.read())
                schema = StructType.fromJson(schema_json)
                print(
                    f"Reading file {output_file_path} using schema: {output_schema_file}"
                )
                spark_session.read.schema(schema).json(
                    path=output_file_path
                ).createOrReplaceTempView(f"expected_{view_name}")
            else:
                spark_session.read.json(
                    path=output_file_path
                ).createOrReplaceTempView(f"expected_{view_name}")
            found_output_file = True
        elif file_extension.lower() == ".parquet":
            spark_session.read.parquet(
                path=output_file_path
            ).createOrReplaceTempView(f"expected_{view_name}")
            found_output_file = True
        if found_output_file:
            # Do a data frame compare on each view
            print(
                f"Comparing with view:[view_name= with view:[expected_{view_name}]"
            )
            # drop any corrupted column
            assert_compare_data_frames(
                expected_df=spark_session.table(f"expected_{view_name}"
                                                ).drop("_corrupt_record"),
                result_df=spark_session.table(view_name)
            )
        return found_output_file

    @staticmethod
    def process_input_file(
        spark_session: SparkSession, input_file: str, input_folder: Path,
        input_schema_folder: Path
    ) -> None:
        file_extension: str = SparkPipelineFrameworkTestRunner.get_file_extension_from_file_path(
            input_file
        )
        view_name: str = SparkPipelineFrameworkTestRunner.get_view_name_from_file_path(
            input_file
        )
        if file_extension.lower() == ".csv":
            input_file_path = os.path.join(input_folder, input_file)
            input_schema_file = os.path.join(
                input_schema_folder, f"{view_name}.json"
            )
            if os.path.exists(input_schema_file):
                with open(input_schema_file) as file:
                    schema_json: str = json.loads(file.read())
                schema = StructType.fromJson(schema_json)
                print(
                    f"Reading file {input_file} using schema: {input_schema_file}"
                )
                spark_session.read.schema(schema).csv(
                    path=input_file_path,
                    header=True,
                    comment="#",
                    emptyValue=None,
                ).limit(SparkPipelineFrameworkTestRunner.row_limit
                        ).createOrReplaceTempView(view_name)
            else:
                spark_session.read.csv(
                    path=input_file_path,
                    header=True,
                    comment="#",
                    emptyValue=None,
                ).limit(SparkPipelineFrameworkTestRunner.row_limit
                        ).createOrReplaceTempView(view_name)
        elif file_extension.lower() == ".jsonl" or file_extension.lower(
        ) == ".json":
            input_file_path = os.path.join(input_folder, input_file)
            # create json_input_folder if it does not exist
            json_input_folder = os.path.join(input_folder, "..", "input_jsonl")
            if not os.path.exists(json_input_folder):
                os.mkdir(json_input_folder)
            jsonl_input_file_path = os.path.join(json_input_folder, input_file)
            # convert file to jsonl if needed
            convert_json_to_jsonl(
                src_file=Path(input_file_path),
                dst_file=Path(jsonl_input_file_path)
            )
            # find schema file
            input_schema_file = os.path.join(input_schema_folder, input_file)
            # if schema exists then use it when loading the file
            if os.path.exists(input_schema_file):
                with open(input_schema_file) as file:
                    schema_json = json.loads(file.read())
                schema = StructType.fromJson(schema_json)
                print(
                    f"Reading file {input_file} using schema: {input_schema_file}"
                )
                spark_session.read.schema(schema).json(
                    path=jsonl_input_file_path
                ).limit(SparkPipelineFrameworkTestRunner.row_limit
                        ).createOrReplaceTempView(view_name)
            else:  # if no schema found just load the file
                spark_session.read.json(path=jsonl_input_file_path).limit(
                    SparkPipelineFrameworkTestRunner.row_limit
                ).createOrReplaceTempView(view_name)

        elif file_extension.lower() == ".parquet":
            spark_session.read.parquet(
                path=os.path.join(input_folder, input_file)
            ).limit(SparkPipelineFrameworkTestRunner.row_limit
                    ).createOrReplaceTempView(view_name)

    @staticmethod
    def get_view_name_from_file_path(input_file: str) -> str:
        view_name: str
        view_name, _ = os.path.splitext(PurePath(input_file).name)
        return view_name

    @staticmethod
    def get_file_extension_from_file_path(input_file: str) -> str:
        file_extension: str
        _, file_extension = os.path.splitext(input_file)
        return file_extension

    @staticmethod
    def write_table_to_output(
        spark_session: SparkSession, view_name: str, output_folder: Path
    ) -> None:
        df: DataFrame = spark_session.table(view_name)
        types: List[Tuple[str, Any]] = df.dtypes
        type_dict: Dict[str, Any] = {key: value for key, value in types}
        # these type strings can look like 'array<struct<Field:string>>', so we
        # have to check if "array" or "struct" appears in the type string, not
        # just for exact matches
        if [t for t in type_dict.values() if "array" in t or "struct" in t]:
            # save as json
            file_path: Path = output_folder.joinpath(
                "temp", f"{view_name}.json"
            )
            print(f"Writing {file_path}")
            df.coalesce(1).write.mode("overwrite").json(path=str(file_path))
            json_files: List[str] = glob.glob(
                str(
                    output_folder.joinpath(
                        "temp", f"{view_name}.json", "*.json"
                    )
                )
            )
            copyfile(
                json_files[0], output_folder.joinpath(f"{view_name}.json")
            )
        else:
            # save as csv
            file_path = output_folder.joinpath("temp", f"{view_name}.csv")
            print(f"Writing {file_path}")

            df.repartition(1).write.mode("overwrite").csv(
                path=str(file_path), header=True
            )
            csv_files: List[str] = glob.glob(
                str(
                    output_folder.joinpath(
                        "temp", f"{view_name}.csv", "*.csv"
                    )
                )
            )
            copyfile(csv_files[0], output_folder.joinpath(f"{view_name}.csv"))

    @staticmethod
    def write_schema_to_output(
        spark_session: SparkSession, view_name: str, schema_folder: Path
    ) -> None:
        df: DataFrame = spark_session.table(view_name)

        # write out schema file if it does not exist
        if not os.path.exists(schema_folder.joinpath(f"{view_name}.json")):
            with open(
                schema_folder.joinpath(f"{view_name}.json"), "w"
            ) as file:
                schema_as_dict: Dict[str, Any] = df.schema.jsonValue()
                # schema_as_dict: Any = json.loads(s=schema_as_json)
                # Adding $schema tag enables auto-complete and syntax checking in editors
                schema_as_dict[
                    "$schema"
                ] = "https://raw.githubusercontent.com/imranq2/SparkPipelineFramework.Testing/main/spark_json_schema" \
                    ".json "
                file.write(json.dumps(schema_as_dict, indent=4))


def get_testable_folders(folder_path: Path) -> List[str]:
    folder_list: List[str] = list_folders(folder_path=folder_path)
    testable_folder_list: List[str] = [
        str(PurePath(folder_path).parent) for folder_path in folder_list
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
