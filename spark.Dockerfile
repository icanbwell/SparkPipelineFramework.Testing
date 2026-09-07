FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.1.11-slim
# https://github.com/icanbwell/helix.spark
# icanbwell/helix.spark is now a private Docker Hub repo (CIE-8032) and this image is
# pulled from the private services-account ECR instead.  Requires an ECR login first:
# `make ecr-login` locally, or aws-actions/amazon-ecr-login in CI.
# The tag is dictated by the pyspark pin in Pipfile (pyspark==3.5.1 -> spark 3.5.1);
# do not bump it here without bumping pyspark across the dependent packages.
USER root

ENV PYTHONPATH=/spftest
ENV CLASSPATH=/spftest/jars:$CLASSPATH

# remove the older version of entrypoints with apt-get because that is how it was installed
RUN apt-get remove python3-entrypoints -y

COPY Pipfile* /spftest/
WORKDIR /spftest

#COPY ./jars/* /opt/spark/jars/
#COPY ./conf/* /opt/spark/conf/
# run this to install any needed jars by Spark
COPY ./test.py ./
RUN /opt/spark/bin/spark-submit --master local[*] test.py

ARG TARGETPLATFORM
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

COPY . /spftest

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs

# Run as non-root user
# Change ownership of the directory and its subdirectories
RUN chown -R spark:spark /spftest

# Set permissions to allow writing (read, write, execute for owner)
RUN chmod -R 755 /spftest
# https://spark.apache.org/docs/latest/running-on-kubernetes.html#user-identity
USER spark
