FROM bitnamilegacy/spark:3.5.0

USER 0

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

RUN set -eux; \
    mkdir -p "${SPARK_HOME}/jars"; \
    curl -fSL -o "${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar" && \
    echo "4ac1cebbe5e9e9af57ff205809950af97b26ba7e  ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" | sha1sum -c -; \
    curl -fSL -o "${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" && \
    echo "48d0f787402ff0b1d32ddefb80d6466f01f4f555  ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" | sha1sum -c -; \
    curl -fSL -o "${SPARK_HOME}/jars/kafka-clients-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar" && \
    echo "54e4beeb4dac3892f5c6cc4141019bb29835acc7  ${SPARK_HOME}/jars/kafka-clients-3.5.0.jar" | sha1sum -c -; \
    curl -fSL -o "${SPARK_HOME}/jars/commons-pool2-2.12.0.jar" "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" && \
    echo "458563f69fbdaebf7daadfe10dc3a22e42a7de50  ${SPARK_HOME}/jars/commons-pool2-2.12.0.jar" | sha1sum -c -; \
    curl -fSL -o "${SPARK_HOME}/jars/delta-spark_2.12-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar" && \
    echo "5eda0aad1fd5c5b4711b732270e8d99d939b2ae8  ${SPARK_HOME}/jars/delta-spark_2.12-3.0.0.jar" | sha1sum -c -; \
    curl -fSL -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    echo "a65839fbf1869f81a1632e09f415e586922e4f80  ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" | sha1sum -c -; \
    curl -fSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.791.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar" && \
    echo "5019ec0be42cd37b630189bbb1d5307c6c9f1c5e  ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.791.jar" | sha1sum -c - && \
    curl -fSL -o "${SPARK_HOME}/jars/hive-metastore-4.1.0.jar" "https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.1.0/hive-metastore-4.1.0.jar" && \
    curl -fSL -o "${SPARK_HOME}/jars/postgresql-42.7.3.jar" "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" && \
    curl -fSL -o "${SPARK_HOME}/jars/delta-storage-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar" && \
    curl -fSL -o "${SPARK_HOME}/jars/spark-avro_2.12-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar"

COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY conf/hive-site.xml ${SPARK_HOME}/conf/hive-site.xml

RUN chown -R 1001:1001 ${SPARK_HOME}/conf

ENV SPARK_JARS_DIR="${SPARK_HOME}/jars"
ENV PYTHONPATH="/opt/spark/jobs:${PYTHONPATH}"

USER 1001
