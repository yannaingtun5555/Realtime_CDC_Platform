FROM flink:1.18.1-scala_2.12-java11

USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir apache-flink==1.18.1 requests && \
    apt-get clean

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop
RUN pip3 install --no-cache-dir kafka-python
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/
RUN chmod 644 /opt/flink/plugins/s3-fs-hadoop/*.jar

# Job 01/02 connectors (not loaded by Flink CLI).
RUN mkdir -p /opt/flink/jars
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.18/flink-connector-kafka-3.2.0-1.18.jar /opt/flink/jars/
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/flink/jars/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar /opt/flink/jars/

# Job 03 only: Iceberg + Hadoop in jars/iceberg (NOT lib/) to avoid CLI classloader clashes.
ENV HADOOP_VERSION=3.3.4
RUN mkdir -p /opt/flink/jars/iceberg
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.6.1/iceberg-flink-runtime-1.18-1.6.1.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/org/apache/commons/commons-configuration2/2.10.1/commons-configuration2-2.10.1.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar /opt/flink/jars/iceberg/
ADD https://repo.maven.apache.org/maven2/com/fasterxml/woodstox/woodstox-core/6.4.0/woodstox-core-6.4.0.jar /opt/flink/jars/iceberg/
RUN chmod 644 /opt/flink/jars/*.jar /opt/flink/jars/iceberg/*.jar

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

USER flink
