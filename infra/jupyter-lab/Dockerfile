FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER $NB_UID
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt


# Download PostgreSQL JDBC driver + Delta Lake JARs (run as root)
USER root
RUN curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.791.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/spark-avro_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
    curl -fSL -o /usr/local/spark-3.5.0-bin-hadoop3/jars/hive-metastore-4.1.0.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.1.0/hive-metastore-4.1.0.jar && \
    chmod 644 /usr/local/spark-3.5.0-bin-hadoop3/jars/postgresql-42.7.3.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/delta-spark_2.12-3.0.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/delta-storage-3.0.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/hadoop-aws-3.3.4.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.791.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/spark-avro_2.12-3.5.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/kafka-clients-3.5.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/commons-pool2-2.12.0.jar \
        /usr/local/spark-3.5.0-bin-hadoop3/jars/hive-metastore-4.1.0.jar

RUN jupyter server extension enable --py jupyterlab_git --sys-prefix

COPY jupyter_lab_config.py /home/jovyan/.jupyter/
COPY conf/spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf
COPY start-jupyter.sh /usr/local/bin/

RUN mkdir -p /home/jovyan/work/examples
RUN mkdir -p /home/jovyan/work/data-engineering
RUN mkdir -p /home/jovyan/work/analytics

USER root
RUN chown -R jovyan:users /home/jovyan/.jupyter && \
    chmod +x /usr/local/bin/start-jupyter.sh
USER $NB_UID

WORKDIR /home/jovyan/work

CMD ["start-jupyter.sh"]
