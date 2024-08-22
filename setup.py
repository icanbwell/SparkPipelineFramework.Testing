# noinspection Mypy
from os import path, getcwd

from setuptools import setup, find_packages

# from https://packaging.python.org/tutorials/packaging-projects/

# noinspection SpellCheckingInspection
package_name = "sparkpipelineframework.testing"

with open("README.md", "r") as fh:
    long_description = fh.read()

try:
    with open(path.join(getcwd(), "VERSION")) as version_file:
        version = version_file.read().strip()
except IOError:
    raise


# classifiers list is here: https://pypi.org/classifiers/

# create the package setup
setup(
    install_requires=[
        "pyspark==3.3.0",
        "protobuf>=3",
        "deprecated>=1.2.12",
        "helix.fhir.client.sdk>=1.0.34",
        "helix-mockserver-client>=1.0.4",
    ],
    name=package_name,
    version=version,
    author="Imran Qureshi",
    author_email="imranq2@hotmail.com",
    description="Testing Framework for SparkPipelineFramework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/imranq2/SparkPipelineFramework.Testing",
    packages=find_packages(
        exclude=[
            "*/test",
            "*/test/*",
            "*tests/*",
            "*library/*",
            "*library",
            "docs",
            "docs/*",
            "docsrc",
            "docsrc/*",
            "keycloak-config",
            "keycloak-config/*",
            ".gihub",
            ".github/*",
        ]
    ),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    dependency_links=[],
    include_package_data=True,
    zip_safe=False,
    package_data={package_name: ["py.typed"]},
    data_files=[],
)
