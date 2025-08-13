FROM apache/airflow:slim-2.10.5-python3.12

USER root
RUN apt-get update -qq && \
    apt-get install -y wget \
    curl \
    nano \
    apt-transport-https \
    gpg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

USER airflow
ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME
USER 50000