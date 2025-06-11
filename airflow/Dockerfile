FROM apache/airflow:2.8.0-python3.9

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt