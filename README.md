[![Build and Test](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/build_and_Test.yml/badge.svg)](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/build_and_Test.yml)

[![Upload Python Package](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/python-publish.yml/badge.svg)](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/python-publish.yml)

[![Known Vulnerabilities](https://snyk.io/test/github/imranq2/SparkPipelineFramework.Testing/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/imranq2/SparkPipelineFramework.Testing?targetFile=requirements.txt)

# SparkPipelineFramework.Tests
Testing framework that can tests SparkPipelineFramework library by just providing input files to setup before running the transformer and output files to use for verifying the output

## Usage
1. Create a folder structure similar to the folder structure of your library in SparkPipelineFramework (This is how the Testing Framework finds the Transformer to run)
2. Create an input folder and put in files that represent the input views.  These files can be csv, json or parquet
3. (Optionally) Create an input_schema folder and put in any schemas you want applied to the above views.  This follows the Spark Json Schema format.
4. (Optional) Create an output folder and put in files that represent the output views you expect.  These files can be csv, json or parquet
5. (Optional) Create an output_schema folder and put in any schemas you want applied to the output views
6. Copy the following test code and put it in a test file in this folder

```python
from pathlib import Path

from pyspark.sql import SparkSession

from spark_pipeline_framework_testing.test_runner import SparkPipelineFrameworkTestRunner


def test_folder(spark_session: SparkSession) -> None:
    data_dir: Path = Path(__file__).parent.joinpath('./')

    SparkPipelineFrameworkTestRunner.run_tests(spark_session=spark_session, folder_path=data_dir)
```
7. Now just run this test.

Note: the test finds files in sub-folders too.

## Example
For the transformer defined here: https://github.com/imranq2/SparkPipelineFramework.Testing/tree/main/library/features/people/my_people_feature
You can find the test here: https://github.com/imranq2/SparkPipelineFramework.Testing/tree/main/tests/library/features/people/my_people_feature

## Publishing a new package
1. Create a new release
2. The GitHub Action should automatically kick in and publish the package
3. You can see the status in the Actions tab
