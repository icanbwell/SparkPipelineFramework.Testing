import os
from pathlib import Path, PurePath
from typing import List


class SparkPipelineFrameworkTestRunner:
    @staticmethod
    def run_tests(folder_path: Path) -> None:
        # iterate through sub_folders trying to find folders that contain input and output folders
        testable_folder_list: List[str] = testable_folders(folder_path=folder_path)
        print(testable_folder_list)
        # for each of them
        # for each file in input folder, load into a view in Spark (use name of file without extension as name of view)
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
