FROM imranq2/helix.spark:3.5.5.0-slim
# https://github.com/icanbwell/helix.spark
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
