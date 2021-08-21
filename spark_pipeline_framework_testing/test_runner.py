import glob
import json
import os
import re
import shutil
from os import listdir
from os.path import isfile, join, isdir
from pathlib import Path, PurePath
from re import search
from typing import List, Optional, Match, Dict, Any, Tuple, Union, Callable, Type

from pyspark.ml import Transformer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Table
from pyspark.sql.types import StructType, DataType
from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
    ExceptionType,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.utilities.class_helpers import ClassHelpers
from spark_pipeline_framework.utilities.json_to_jsonl_converter import (
    convert_json_to_jsonl,
)

from spark_pipeline_framework_testing.testing_exception import (
    SparkPipelineFrameworkTestingException,
)


class SparkPipelineFrameworkTestRunner:
    row_limit: int = 100

    @staticmethod
    def run_tests(
        spark_session: SparkSession,
        folder_path: Path,
        parameters: Optional[Dict[str, Any]] = None,
        func_path_modifier: Optional[
            Callable[[Union[Path, str]], Union[Path, str]]
        ] = None,
        temp_folder: Optional[Path] = None,
        transformer_type: Optional[Type[Transformer]] = None,
        sort_output_by: Optional[List[str]] = None,
        output_as_json_only: bool = True,
        apply_schema_to_output: bool = True,
        check_output: bool = True,
        ignore_views_for_output: Optional[List[str]] = None,
        input_schema: Optional[
            Union[StructType, Dict[str, StructType], DataType]
        ] = None,
        output_schema: Optional[
            Union[StructType, Dict[str, StructType], DataType]
        ] = None,
    ) -> None:
        """
        Tests Spark Transformers without writing any code

        1. Reads all the files in the `input` folder into Spark views (using filename as view name)
        2. If input_schema folder is present then it uses those schemas when loading the files in input folder.
        3. Runs the Spark Transformer at the same path in the project.  E.g., if your test is spf_tests/library/foo then
            it will look for a transformer in library/foo. So basically it takes the path after `library` and looks
            for a Transformer at that location
        4. If output files or output_schema files are not present then this writes them out
        5. If output files are present then this compares the actual view in Spark (after running the Transformer above)
            with the view stored in the output file
        6. If output_schema is present then it uses that when loading the output file


        :param spark_session: Spark Session
        :param folder_path: where to look for test files
        :param parameters: (Optional) any parameters to pass to the transformer
        :param func_path_modifier: (Optional) A function that can transform the paths
        :param temp_folder: folder to use for temporary files.  Any existing files in this folder will be deleted.
        :param transformer_type: (Optional) the transformer to run
        :param sort_output_by: (Optional) sort by these columns before comparing or writing output files
        :param output_as_json_only: (Optional) if set to True then do not output as csv
        :param apply_schema_to_output: If true applies schema to output file
        :param check_output: if set, check the output of the test.  Otherwise don't check the output.
        :param ignore_views_for_output: list of view names to ignore when writing output schema and output json
        :param input_schema: Optional input_schema to apply to the input data. Can be a schema or a dictionary
                                of schemas where the key is name of the view
        :param output_schema: Optional output_schema to apply to the input data. Can be a schema or a dictionary
                                of schemas where the key is name of the view
        :return: Throws SparkPipelineFrameworkTestingException if there are mismatches between
                    expected output files and actual output files.  The `exceptions` list in
                    SparkPipelineFrameworkTestingException holds all the mismatch exceptions
        """
        if not parameters:
            parameters = {}

        # iterate through sub_folders trying to find folders that contain input and output folders
        testable_folder_list: List[str] = get_testable_folders(folder_path=folder_path)

        # first clear any stuff in SparkSession
        clean_spark_session(session=spark_session)
        input_table_names: List[str] = []

        # for each of them
        testable_folder: str
        for testable_folder in testable_folder_list:
            print(f"Running test in folder: {testable_folder}...")

            input_folder: Path = Path(testable_folder).joinpath("input")
            input_files: List[str] = []
            if isdir(input_folder):
                input_files = [
                    f for f in listdir(input_folder) if isfile(join(input_folder, f))
                ]
            input_schema_folder = Path(testable_folder).joinpath("input_schema")

            # for each file in input folder, load into a view in Spark
            #   (use name of file without extension as name of view)
            for input_file in input_files:
                SparkPipelineFrameworkTestRunner.process_input_file(
                    spark_session=spark_session,
                    input_file=input_file,
                    input_folder=input_folder,
                    input_schema_folder=input_schema_folder,
                    input_schema=input_schema,
                )

            # write out any input schemas
            input_tables: List[Table] = spark_session.catalog.listTables("default")

            input_table_names = [
                t.name for t in input_tables if not t.name.startswith("expected_")
            ]
            if not input_schema:
                table_name: str
                for table_name in input_table_names:
                    if not os.path.exists(input_schema_folder):
                        os.mkdir(input_schema_folder)

                    SparkPipelineFrameworkTestRunner.write_schema_to_output(
                        spark_session=spark_session,
                        view_name=table_name,
                        schema_folder=input_schema_folder,
                    )

        # read parameters.json if it exists
        parameters_json_file: Path = Path(folder_path).joinpath("parameters.json")
        if os.path.exists(parameters_json_file):
            with open(parameters_json_file, "r") as file:
                parameters = json.loads(file.read())
                assert parameters

        # turn path into transformer name and call transformer
        # first set the view parameter since AutoMapper transformers require it
        if "view" not in parameters:
            destination_view_name: str = "output"
            parameters["view"] = destination_view_name

        # find name of transformer and run it
        search_result: Optional[Match[str]] = search(r"/library/", str(folder_path))
        if search_result:
            SparkPipelineFrameworkTestRunner.run_transformer(
                spark_session=spark_session,
                parameters=parameters,
                search_result=search_result,
                testable_folder=str(folder_path),
                transformer_type=transformer_type,
            )

        if check_output:
            # write out any missing schemas
            output_tables: List[Table] = spark_session.catalog.listTables("default")

            if ignore_views_for_output is not None:
                output_tables = [
                    table
                    for table in output_tables
                    if table.name not in ignore_views_for_output
                ]

            output_schema_folder: Path = Path(folder_path).joinpath("output_schema")
            tables_for_writing_schema: List[str] = [
                t.name
                for t in output_tables
                if not t.name.startswith("expected_")
                and t.name not in input_table_names
            ]
            if (
                "output" in tables_for_writing_schema
            ):  # if there is an output table then ignore other input_tables
                tables_for_writing_schema = ["output"]

            if not output_schema:
                for table_name in tables_for_writing_schema:
                    if not os.path.exists(output_schema_folder):
                        os.mkdir(output_schema_folder)

                    SparkPipelineFrameworkTestRunner.write_schema_to_output(
                        spark_session=spark_session,
                        view_name=table_name,
                        schema_folder=output_schema_folder,
                    )

            # for each file in output folder, loading into a view in Spark (prepend with "expected_")
            output_folder = Path(folder_path).joinpath("output")
            if not os.path.exists(output_folder):
                os.mkdir(output_folder)
            output_files = [
                f for f in listdir(output_folder) if isfile(join(output_folder, f))
            ]
            views_found: List[str] = []
            if not temp_folder:
                temp_folder = output_folder.joinpath("temp")

            if os.path.exists(temp_folder):
                shutil.rmtree(temp_folder)

            data_frame_exceptions: List[SparkDataFrameComparerException] = []
            for output_file in output_files:
                found_output_file: bool
                data_frame_exception: Optional[SparkDataFrameComparerException]
                (
                    found_output_file,
                    data_frame_exception,
                ) = SparkPipelineFrameworkTestRunner.process_output_file(
                    spark_session=spark_session,
                    output_file=output_file,
                    output_folder=output_folder,
                    output_schema_folder=output_schema_folder,
                    temp_folder=temp_folder.joinpath("result"),
                    func_path_modifier=func_path_modifier,
                    sort_output_by=sort_output_by,
                    apply_schema_to_output=apply_schema_to_output,
                    output_schema=output_schema,
                )
                if found_output_file:
                    views_found.append(
                        SparkPipelineFrameworkTestRunner.get_view_name_from_file_path(
                            output_file
                        ).lower()
                    )
                    if data_frame_exception:
                        data_frame_exceptions.append(data_frame_exception)

            # write out any missing output files
            table_names_to_write_to_output: List[str] = [
                t.name
                for t in output_tables
                if t.name.lower() not in views_found
                and not t.name.startswith("expected_")
                and t.name not in input_table_names
            ]
            if (
                "output" in table_names_to_write_to_output
            ):  # if there is an output table then ignore other input_tables
                table_names_to_write_to_output = ["output"]
            for table_name in table_names_to_write_to_output:
                SparkPipelineFrameworkTestRunner.write_table_to_output(
                    spark_session=spark_session,
                    view_name=table_name,
                    output_folder=output_folder,
                    temp_folder=temp_folder.joinpath("result"),
                    sort_output_by=sort_output_by,
                    output_as_json_only=output_as_json_only,
                )

            clean_spark_session(session=spark_session)

            if len(data_frame_exceptions) > 0:
                raise SparkPipelineFrameworkTestingException(
                    exceptions=data_frame_exceptions
                )

    @staticmethod
    def run_transformer(
        spark_session: SparkSession,
        parameters: Optional[Dict[str, Any]],
        search_result: Match[str],
        testable_folder: str,
        transformer_type: Optional[Type[Transformer]],
    ) -> None:
        my_class: Type[Transformer]
        if transformer_type:
            my_class = transformer_type
        else:
            # get name of transformer file
            search_result_end = search_result.end()
            transformer_file_name: str = testable_folder[search_result_end:].replace(
                "/", "_"
            )
            # find parent folder of transformer file
            search_result_start_ = search_result.start() + 1
            lib_path: str = testable_folder[search_result_start_:].replace("/", ".")
            # load the transformer file (i.e., module)
            full_reference = lib_path + "." + transformer_file_name
            my_class = ClassHelpers.get_first_class_in_file(full_reference)

        with ProgressLogger() as progress_logger:
            # now figure out the class_parameters to use when instantiating the class
            class_parameters: Dict[str, Any] = {
                "parameters": parameters or {},
                "progress_logger": progress_logger,
            }
            my_instance: Transformer = ClassHelpers.instantiate_class_with_parameters(
                class_parameters=class_parameters, my_class=my_class
            )
            # now call transform
            schema = StructType([])
            # create an empty dataframe to pass into transform()
            df: DataFrame = spark_session.createDataFrame(
                spark_session.sparkContext.emptyRDD(), schema
            )
            my_instance.transform(df)

    @staticmethod
    def process_output_file(
        spark_session: SparkSession,
        output_file: str,
        output_folder: Path,
        output_schema_folder: Path,
        func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]],
        sort_output_by: Optional[List[str]],
        apply_schema_to_output: bool,
        output_schema: Optional[Union[StructType, Dict[str, StructType], DataType]],
        temp_folder: Optional[Union[Path, str]] = None,
    ) -> Tuple[bool, Optional[SparkDataFrameComparerException]]:
        data_frame_exception: Optional[SparkDataFrameComparerException] = None
        file_extension: str = (
            SparkPipelineFrameworkTestRunner.get_file_extension_from_file_path(
                output_file
            )
        )
        view_name: str = SparkPipelineFrameworkTestRunner.get_view_name_from_file_path(
            output_file
        )
        if file_extension.lower() not in [".csv", ".json", ".jsonl", ".parquet"]:
            return True, data_frame_exception
        result_df: DataFrame = spark_session.table(view_name)
        sort_columns: List[str] = (
            [col for col in sort_output_by if col in result_df.columns]
            if sort_output_by
            else []
        )

        found_output_file: bool
        output_file_path = os.path.join(output_folder, output_file)
        result_path: Optional[Path] = (
            Path(temp_folder).joinpath(f"{view_name}") if temp_folder else None
        )

        # get schema
        output_schema_for_view: Optional[StructType] = None
        output_schema_file: str = os.path.join(
            output_schema_folder, f"{view_name}.json"
        )
        # if there is a schema file and no schema was passed in then use that
        if (
            apply_schema_to_output
            and not output_schema
            and os.path.exists(output_schema_file)
        ):
            with open(output_schema_file) as file:
                schema_json = json.loads(file.read())
            output_schema_for_view = StructType.fromJson(schema_json)
            print(f"Reading file {output_file_path} using schema: {output_schema_file}")
        elif apply_schema_to_output and output_schema:
            print(
                f"Reading file {output_file_path} using passed in schema for view {view_name}"
            )
            # the schema is passed either as a single schema or a dict of schemas
            output_schema_for_view = (
                output_schema[view_name]  # type: ignore
                if output_schema
                and isinstance(output_schema, dict)
                and view_name in output_schema
                else output_schema
            )

        # create a reader to read the file (using schema if specified)
        reader = (
            spark_session.read.schema(output_schema_for_view)
            if output_schema_for_view
            else spark_session.read
        )

        # now read the file using the reader for the file extension
        output_df: DataFrame
        if file_extension.lower() == ".csv":
            output_df = reader.csv(
                path=output_file_path, header=True, comment="#", emptyValue=None
            )
            found_output_file = True
        elif file_extension.lower() == ".jsonl" or file_extension.lower() == ".json":
            output_df = reader.option("multiLine", True).json(path=output_file_path)
            found_output_file = True
        elif file_extension.lower() == ".parquet":
            output_df = reader.parquet(path=output_file_path)
            found_output_file = True
        else:
            assert False, f"Unsupported file extension: {file_extension}"

        # create expected view
        output_df.createOrReplaceTempView(f"expected_{view_name}")

        # write result to temp folder for comparison
        if result_path and temp_folder:
            result_path_for_view = result_path.joinpath(f"{view_name}.json")
            result_df = (
                result_df.coalesce(1).sort(*sort_columns)
                if len(sort_columns) > 0
                else result_df.coalesce(1)
            )
            result_df.write.json(path=str(result_path_for_view))
            if output_schema and output_schema_for_view:
                result_df = result_df.sql_ctx.read.schema(output_schema_for_view).json(
                    str(result_path_for_view)
                )
            result_file: Path = Path(temp_folder).joinpath(f"{view_name}.json")
            if file_extension.lower() == ".csv":
                SparkPipelineFrameworkTestRunner.combine_spark_csv_files_to_one_file(
                    source_folder=result_path_for_view,
                    destination_file=result_file,
                    file_extension="csv",
                )
            elif (
                file_extension.lower() == ".jsonl" or file_extension.lower() == ".json"
            ):
                SparkPipelineFrameworkTestRunner.combine_spark_json_files_to_one_file(
                    source_folder=result_path_for_view,
                    destination_file=result_file,
                    file_extension="json",
                )

            if found_output_file:
                # Do a data frame compare on each view
                print(
                    f"Comparing with view:[view_name= with view:[expected_{view_name}]"
                )
                try:
                    # drop any corrupted column
                    assert_compare_data_frames(
                        expected_df=spark_session.table(f"expected_{view_name}").drop(
                            "_corrupt_record"
                        ),
                        result_df=result_df,
                        result_path=result_file,
                        expected_path=output_file_path,
                        temp_folder=temp_folder,
                        func_path_modifier=func_path_modifier,
                        order_by=sort_columns if len(sort_columns) > 0 else None,
                    )
                except SparkDataFrameComparerException as e:
                    data_frame_exception = e
                    # for schema errors, show a compare path for schema
                    if e.exception_type == ExceptionType.SchemaMismatch:
                        if temp_folder and output_schema_file:
                            # write the new schema to temp folder
                            result_schema_path = (
                                SparkPipelineFrameworkTestRunner.write_schema_to_output(
                                    spark_session=spark_session,
                                    view_name=view_name,
                                    schema_folder=Path(temp_folder)
                                    .joinpath("schemas")
                                    .joinpath("result")
                                    .joinpath(view_name),
                                )
                            )
                            e.compare_path = (
                                SparkPipelineFrameworkTestRunner.get_compare_path(
                                    result_path=result_schema_path,
                                    expected_path=Path(output_schema_file),
                                    temp_folder=temp_folder,
                                    func_path_modifier=func_path_modifier,
                                    type_="schema",
                                )
                            )
                            if func_path_modifier and e.compare_path:
                                e.compare_path = func_path_modifier(e.compare_path)

        return found_output_file, data_frame_exception

    @staticmethod
    def get_compare_path(
        result_path: Optional[Path],
        expected_path: Optional[Path],
        temp_folder: Optional[Union[Path, str]],
        func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]],
        type_: str,
    ) -> Optional[Path]:
        compare_sh_path: Optional[Path] = None
        if expected_path and result_path and temp_folder:
            expected_file_name: str = os.path.basename(expected_path)
            # create a temp file to launch the diff tool
            # use .command:
            # https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
            compare_sh_path = Path(temp_folder).joinpath(
                f"compare_{type_}_{expected_file_name}.command"
            )
            with open(compare_sh_path, "w") as compare_sh:
                compare_sh.write(
                    f"/usr/local/bin/charm diff "
                    f"{func_path_modifier(result_path) if func_path_modifier else result_path} "
                    f"{func_path_modifier(expected_path) if func_path_modifier else expected_path}"
                )
                os.fchmod(compare_sh.fileno(), 0o7777)
        return compare_sh_path

    @staticmethod
    def process_input_file(
        spark_session: SparkSession,
        input_file: str,
        input_folder: Path,
        input_schema_folder: Path,
        input_schema: Optional[Union[StructType, Dict[str, StructType], DataType]],
    ) -> None:
        file_extension: str = (
            SparkPipelineFrameworkTestRunner.get_file_extension_from_file_path(
                input_file
            )
        )
        if file_extension.lower() not in [".csv", ".json", ".jsonl", ".parquet"]:
            return

        view_name: str = SparkPipelineFrameworkTestRunner.get_view_name_from_file_path(
            input_file
        )

        input_file_path = os.path.join(input_folder, input_file)

        # get schema
        input_schema_for_view: Optional[StructType] = None
        input_schema_file: str = os.path.join(input_schema_folder, f"{view_name}.json")
        # if there is a schema file and no schema was passed in then use that
        if not input_schema and os.path.exists(input_schema_file):
            with open(input_schema_file, "r") as file:
                schema_json = json.loads(file.read())
            input_schema_for_view = StructType.fromJson(schema_json)
            print(f"Reading file {input_file_path} using schema: {input_schema_file}")
        elif input_schema:
            print(
                f"Reading file {input_file_path} using passed in schema for view {view_name}"
            )
            # the schema is passed either as a single schema or a dict of schemas
            input_schema_for_view = (
                input_schema[view_name]  # type: ignore
                if input_schema
                and isinstance(input_schema, dict)
                and view_name in input_schema
                else input_schema
            )

        # create a reader to read the file (using schema if specified)
        reader = (
            spark_session.read.schema(input_schema_for_view)
            if input_schema_for_view
            else spark_session.read
        )

        input_df: DataFrame
        if file_extension.lower() == ".csv":
            input_df = reader.csv(
                path=input_file_path,
                header=True,
                comment="#",
                emptyValue=None,
            ).limit(SparkPipelineFrameworkTestRunner.row_limit)
        elif file_extension.lower() == ".jsonl" or file_extension.lower() == ".json":
            # create json_input_folder if it does not exist
            json_input_folder = os.path.join(input_folder, "..", "input_jsonl")
            if not os.path.exists(json_input_folder):
                os.mkdir(json_input_folder)
            jsonl_input_file_path = os.path.join(json_input_folder, input_file)
            # convert file to jsonl if needed
            convert_json_to_jsonl(
                src_file=Path(input_file_path), dst_file=Path(jsonl_input_file_path)
            )
            input_df = reader.json(path=jsonl_input_file_path).limit(
                SparkPipelineFrameworkTestRunner.row_limit
            )
        elif file_extension.lower() == ".parquet":
            input_df = reader.parquet(path=input_file_path).limit(
                SparkPipelineFrameworkTestRunner.row_limit
            )
        else:
            assert False, f"Unsupported file extension: {file_extension}"

        # create expected view
        input_df.createOrReplaceTempView(view_name)
        assert (
            "_corrupt_record" not in spark_session.table(view_name).columns
        ), input_file_path

    @staticmethod
    def get_view_name_from_file_path(input_file: str) -> str:
        view_name: str
        view_name, _ = os.path.splitext(PurePath(input_file).name)
        cleaned_view_name = re.sub(r"-", "_", view_name)
        return cleaned_view_name

    @staticmethod
    def get_file_extension_from_file_path(file_name: str) -> str:
        file_extension: str
        _, file_extension = os.path.splitext(file_name)
        return file_extension

    @staticmethod
    def should_write_dataframe_as_json(df: DataFrame) -> bool:
        types: List[Tuple[str, Any]] = df.dtypes
        type_dict: Dict[str, Any] = {key: value for key, value in types}
        # these type strings can look like 'array<struct<Field:string>>', so we
        # have to check if "array" or "struct" appears in the type string, not
        # just for exact matches
        return any([t for t in type_dict.values() if "array" in t or "struct" in t])

    @staticmethod
    def write_table_to_output(
        spark_session: SparkSession,
        view_name: str,
        output_folder: Path,
        temp_folder: Path,
        sort_output_by: Optional[List[str]],
        output_as_json_only: bool,
    ) -> None:
        df: DataFrame = spark_session.table(view_name)
        sort_columns: List[str] = (
            [col for col in sort_output_by if col in df.columns]
            if sort_output_by
            else []
        )
        if (
            output_as_json_only
            or SparkPipelineFrameworkTestRunner.should_write_dataframe_as_json(df=df)
        ):
            # save as json
            file_path: Path = temp_folder.joinpath(f"{view_name}.json")
            print(f"Writing {file_path}")
            if len(sort_columns) > 0:
                df.coalesce(1).sort(*sort_columns).write.mode("overwrite").json(
                    path=str(file_path)
                )
            else:
                df.coalesce(1).write.mode("overwrite").json(path=str(file_path))
            SparkPipelineFrameworkTestRunner.combine_spark_json_files_to_one_file(
                source_folder=file_path,
                destination_file=output_folder.joinpath(f"{view_name}.json"),
                file_extension="json",
            )
        else:
            # save as csv
            file_path = temp_folder.joinpath(f"{view_name}.csv")
            print(f"Writing {file_path}")

            if len(sort_columns) > 0:
                df.coalesce(1).sort(*sort_columns).write.mode("overwrite").csv(
                    path=str(file_path), header=True
                )
            else:
                df.coalesce(1).write.mode("overwrite").csv(
                    path=str(file_path), header=True
                )
            SparkPipelineFrameworkTestRunner.combine_spark_csv_files_to_one_file(
                source_folder=file_path,
                destination_file=output_folder.joinpath(f"{view_name}.csv"),
                file_extension="csv",
            )

    @staticmethod
    def combine_spark_csv_files_to_one_file(
        source_folder: Path, destination_file: Path, file_extension: str
    ) -> None:
        file_pattern_to_search: Path = source_folder.joinpath(f"*.{file_extension}")
        # find files with that extension in source_folder
        files: List[str] = glob.glob(str(file_pattern_to_search))
        lines: List[str] = []
        for file in files:
            with open(file, "r") as file_source:
                lines = lines + file_source.readlines()

        with open(destination_file, "w") as file_destination:
            file_destination.writelines(lines)
            file_destination.write("\n")

    @staticmethod
    def combine_spark_json_files_to_one_file(
        source_folder: Path, destination_file: Path, file_extension: str
    ) -> None:
        file_pattern_to_search: Path = source_folder.joinpath(f"*.{file_extension}")
        # find files with that extension in source_folder
        files: List[str] = glob.glob(str(file_pattern_to_search))
        # now copy the first file to the destination
        lines: List[str] = []
        for file in files:
            with open(file, "r") as file_source:
                lines = lines + file_source.readlines()

        # convert from json to json and write in pretty print
        os.makedirs(os.path.dirname(destination_file), exist_ok=True)
        with open(destination_file, "w") as file_destination:
            json_array: List[Any] = [json.loads(line) for line in lines]
            file_destination.write(json.dumps(json_array, indent=2))
            file_destination.write("\n")

    @staticmethod
    def write_schema_to_output(
        spark_session: SparkSession, view_name: str, schema_folder: Path
    ) -> Path:
        df: DataFrame = spark_session.table(view_name)

        # write out schema file if it does not exist
        schema_file_path: Path = schema_folder.joinpath(f"{view_name}.json")
        if not os.path.exists(schema_file_path):
            os.makedirs(os.path.dirname(schema_file_path), exist_ok=True)
            with open(schema_file_path, "w") as file:
                schema_as_dict: Dict[str, Any] = df.schema.jsonValue()
                # schema_as_dict: Any = json.loads(s=schema_as_json)
                # Adding $schema tag enables auto-complete and syntax checking in editors
                schema_as_dict["$schema"] = (
                    "https://raw.githubusercontent.com/imranq2/SparkPipelineFramework.Testing/main/spark_json_schema"
                    ".json "
                )
                file.write(json.dumps(schema_as_dict, indent=4))
        return schema_file_path


def get_testable_folders(folder_path: Path) -> List[str]:
    folder_list: List[str] = list_folders(folder_path=folder_path)
    testable_folder_list: List[str] = [
        str(PurePath(folder_path).parent)
        for folder_path in folder_list
        if PurePath(folder_path).name in ["input", "source_api_calls"]
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
