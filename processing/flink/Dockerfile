FROM flink:1.20.0-scala_2.12-java17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --no-cache-dir apache-flink==1.20.0 \
    && mkdir -p /opt/flink/lib/mdep

# Exact connector artifacts used by processing/flink/flink_cdc_job.py. The
# Flink Kafka connector is deliberately supplied by the runtime image rather
# than assumed to exist in the upstream binary image.
ARG FLINK_KAFKA_VERSION=3.3.0-1.20
ARG ICEBERG_VERSION=1.6.1
RUN curl --fail --location --retry 3 \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_KAFKA_VERSION}/flink-connector-kafka-${FLINK_KAFKA_VERSION}.jar" \
      --output "/opt/flink/lib/flink-connector-kafka-${FLINK_KAFKA_VERSION}.jar" \
    && curl --fail --location --retry 3 \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" \
      --output "/opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"

USER flink
