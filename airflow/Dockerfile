FROM apache/airflow:2.8.0-python3.9

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz && \
    mkdir -p ${SPARK_HOME} && \
    tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 && \
    rm /tmp/spark.tgz

USER airflow

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt