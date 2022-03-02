FROM imranq2/spark-py:3.0.49
# https://github.com/imranq2/kubernetes.spark_python
USER root

ENV PYTHONPATH=/spftest
ENV CLASSPATH=/spftest/jars:$CLASSPATH

COPY Pipfile* /spftest/
WORKDIR /spftest

RUN df -h # for space monitoring
RUN pipenv sync --dev --system

# COPY ./jars/* /opt/bitnami/spark/jars/
# COPY ./conf/* /opt/bitnami/spark/conf/

COPY . /spftest

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
# USER 1001
