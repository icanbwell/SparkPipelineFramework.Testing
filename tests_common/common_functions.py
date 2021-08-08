import json
import os
import re
from pathlib import Path, PurePath
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame


def camel_case_to_snake_case(text: str) -> str:
    import re

    str1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", str1).lower()


def get_folders_containing_testcases(
    folder_path: Path, test_input_directories: List[str] = ["input"]
) -> List[str]:
    folder_list: List[str] = list_folders(folder_path=folder_path)
    testable_folder_list: List[str] = [
        str(PurePath(folder_path).parent)
        for folder_path in folder_list
        if PurePath(folder_path).name in test_input_directories
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


def get_file_extension_from_file_path(file_name: str) -> str:
    file_extension: str
    _, file_extension = os.path.splitext(file_name)
    return file_extension


def get_view_name_from_file_path(input_file: str) -> str:
    view_name: str
    view_name, _ = os.path.splitext(PurePath(input_file).name)
    cleaned_view_name = re.sub(r"-", "_", view_name)
    return cleaned_view_name
