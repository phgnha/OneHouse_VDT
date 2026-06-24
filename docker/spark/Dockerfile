FROM python:3.12-slim-bookworm

USER root

# 1. System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Environment
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 3. Copy locally downloaded Spark
COPY spark.tgz /tmp/spark.tgz
RUN tar -xf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# 4. Download Iceberg JARs (Iceberg 1.7.1, compatible with Spark 3.5.x)
RUN curl -L --retry 5 --retry-delay 5 \
    -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar && \
    curl -L --retry 5 --retry-delay 5 \
    -o ${SPARK_HOME}/jars/iceberg-aws-bundle-1.7.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

# 5. Download Kafka connector JARs (for Spark 3.5.3)
RUN curl -L --retry 5 --retry-delay 5 \
    -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -L --retry 5 --retry-delay 5 \
    -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -L --retry 5 --retry-delay 5 \
    -o ${SPARK_HOME}/jars/kafka-clients-3.7.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
    curl -L --retry 5 --retry-delay 5 \
    -o ${SPARK_HOME}/jars/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# 6. Install PySpark (must match SPARK_VERSION)
RUN pip install --no-cache-dir pyspark==3.5.3

# 7. Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR ${SPARK_HOME}

ENTRYPOINT ["/entrypoint.sh"]
