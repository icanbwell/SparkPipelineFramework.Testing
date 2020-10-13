import os
from os import listdir
from os.path import isfile, join
from pathlib import Path, PurePath
from typing import List

from pyspark.sql import SparkSession


class SparkPipelineFrameworkTestRunner:
    @staticmethod
    def run_tests(spark_session: SparkSession, folder_path: Path) -> None:
        # iterate through sub_folders trying to find folders that contain input and output folders
        testable_folder_list: List[str] = testable_folders(folder_path=folder_path)
        print(testable_folder_list)
        # for each of them
        for testable_folder in testable_folder_list:
            input_folder: Path = Path(testable_folder).joinpath("input")
            input_files: List[str] = [f for f in listdir(input_folder) if isfile(join(input_folder, f))]

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
                    spark_session.read.csv(path=os.path.join(input_folder, input_file),
                                           header=True).createOrReplaceTempView(filename)
            # turn path into transformer name
            # call transformer
            # for each file in output folder, loading into a view in Spark (prepend with "expected_")
            # Do a data frame compare on each


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
