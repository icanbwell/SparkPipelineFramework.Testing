[![Build and Test](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/build_and_Test.yml/badge.svg)](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/build_and_Test.yml)

[![Upload Python Package](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/python-publish.yml/badge.svg)](https://github.com/imranq2/SparkPipelineFramework.Testing/actions/workflows/python-publish.yml)

[![Known Vulnerabilities](https://snyk.io/test/github/imranq2/SparkPipelineFramework.Testing/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/imranq2/SparkPipelineFramework.Testing?targetFile=requirements.txt)

# SparkPipelineFramework.Tests
Testing framework that can tests SparkPipelineFramework library by just providing input files to setup before running the transformer and output files to use for verifying the output

## Local development setup

The `spark.Dockerfile` and `pre-commit.Dockerfile` base image (`helix.spark`) lives in a
**private ECR repo in the b.well services account**
(`856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark`) rather than on Docker Hub -
see [CIE-8032](https://icanbwell.atlassian.net/browse/CIE-8032). Before building anything:

1. `aws sso login --profile services`
   Pass `AWS_SERVICES_PROFILE=<name>` to `make` if your profile is not called `services`.
2. `make build` / `make devdocker` / `make up` / `make run-pre-commit` log in to the ECR
   for you. If you build by another route (a bare `docker compose build`, or the git
   pre-commit hook), run `make ecr-login` first.

A `pull access denied` or `no basic auth credentials` error from `docker build` means the
ECR login has expired - re-run step 1 and `make ecr-login`.

Note: because the base image is private, **external contributors and forks cannot build
the Docker images.** Pure-Python changes can still be developed and unit-tested without
Docker; the containerised `make tests` / `make run-pre-commit` targets cannot.

When bumping the `helix.spark` tag, remember its publish workflow does not push to this
ECR - the new tag has to be copied into `856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark`
manually first, or the build fails on a missing image. The tag must also stay in step with
the `pyspark` pin in `Pipfile` (currently `==3.5.1`, matching `helix.spark:3.5.1.11`).

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
