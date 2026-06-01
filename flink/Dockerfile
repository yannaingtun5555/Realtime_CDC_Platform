FROM flink:1.18.1-scala_2.12-java11

USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir apache-flink==1.18.1 requests && \
    apt-get clean

# 1. CREATE PLUGINS DIRECTORY FOR S3 HADOOP
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop
RUN pip3 install --no-cache-dir kafka-python
# 2. DOWNLOAD THE S3 FILE SYSTEM PLUGIN (Matches your Flink version 1.18.1)
# Note: Keep ONLY this jar here to avoid classpath pollution!
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/
RUN chmod 644 /opt/flink/plugins/s3-fs-hadoop/*.jar

# Iceberg on global classpath (required for CREATE CATALOG during flink run -py).
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.6.1/iceberg-flink-runtime-1.18-1.6.1.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/iceberg-flink-runtime-1.18-1.6.1.jar

RUN mkdir -p /opt/flink/jars
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.18/flink-connector-kafka-3.2.0-1.18.jar /opt/flink/jars/
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/flink/jars/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar /opt/flink/jars/
RUN chmod 644 /opt/flink/jars/*.jar
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && \
chmod +x /usr/local/bin/mc
# Switch back to flink user for safety
USER flink