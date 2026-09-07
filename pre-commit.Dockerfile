FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.1.11-precommit-slim
# https://github.com/icanbwell/helix.spark
# Same private services-account ECR as spark.Dockerfile (CIE-8032) - needs an ECR login
# first (`make ecr-login` locally, aws-actions/amazon-ecr-login in CI).
#
# This image carries no Spark at all: it is python:3.12-slim + git + pipenv +
# build-essential.  It could therefore be rebased onto
# 856965016623.dkr.ecr.us-east-1.amazonaws.com/root-mirror/python:3.12-slim, which is the
# only base that also clears Aikido CUSTOM-RULE-2300 ("not sourced from root.io ECR
# mirror").  Not done here because the presence of that mirrored tag is unverified,
# whereas helix.spark:3.5.1.11-precommit-slim is known-present in this ECR.

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY Pipfile* ./

ARG TARGETPLATFORM
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD ["pre-commit", "run", "--all-files"]

# don't default to root.  pre-commit-hook already overrides this at runtime with
# --user "$(id -u):$(id -g)" so the bind-mounted /sourcecode stays writable
# whatever UID the host uses.
USER 1001
