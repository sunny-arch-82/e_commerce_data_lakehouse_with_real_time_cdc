# Stage 1: download JARs
FROM python:3.11-slim AS downloader
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /deps && \
    curl -sL "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" -o /deps/mysql-connector-j-8.0.33.jar && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o /deps/hadoop-aws-3.3.4.jar && \
    curl -sL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o /deps/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.9.2/iceberg-hive-runtime-1.9.2.jar" -o /deps/iceberg-hive-runtime-1.9.2.jar

# Stage 2: final image
FROM apache/hive:standalone-metastore-4.1.0
USER root
COPY --from=downloader /deps/*.jar /opt/hive/lib/
COPY --from=downloader /deps/*.jar /opt/hadoop/share/hadoop/common/lib/
COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY conf/core-site.xml /opt/hive/conf/core-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod 644 /opt/hive/lib/mysql-connector-j-8.0.33.jar \
    /opt/hive/lib/hadoop-aws-3.3.4.jar \
    /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
    /opt/hive/lib/iceberg-hive-runtime-1.9.2.jar \
    /opt/hadoop/share/hadoop/common/lib/mysql-connector-j-8.0.33.jar \
    /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.4.jar \
    /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.262.jar \
    /opt/hadoop/share/hadoop/common/lib/iceberg-hive-runtime-1.9.2.jar \
    /opt/hive/conf/hive-site.xml \
    /opt/hive/conf/core-site.xml && \
    chmod +x /entrypoint.sh
USER hive
ENTRYPOINT ["/entrypoint.sh"]
