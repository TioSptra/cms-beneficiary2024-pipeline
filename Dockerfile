FROM apache/airflow:slim-3.0.2-python3.12

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

# install java from adoptium temurin java
RUN wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null
RUN echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
RUN apt-get update -qq && \
    apt-get install -y temurin-17-jdk && \
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