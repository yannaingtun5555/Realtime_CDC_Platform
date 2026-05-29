FROM flink:1.18.1-scala_2.12-java11

USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir protobuf && \
    apt-get clean

# Create plugin directory for S3 filesystem
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop

# Download S3 plugin and its dependencies INTO THE PLUGIN DIRECTORY
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/flink/plugins/s3-fs-hadoop/
ADD https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/flink/plugins/s3-fs-hadoop/

# Other JARs (Kafka, Iceberg) can stay in lib/
RUN mkdir -p /opt/flink/lib/iceberg
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.6.1/iceberg-flink-runtime-1.18-1.6.1.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.18/flink-connector-kafka-3.2.0-1.18.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar /opt/flink/lib/

# Optional: if you also want to keep the S3 JAR in lib (not needed but harmless), you can symlink