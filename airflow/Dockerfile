FROM apache/airflow:2.7.2-python3.11

# PySpark needs a Java runtime (Member 2 ETL + Member 3 inference + quality checks)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Stable JAVA_HOME regardless of CPU architecture (amd64 / arm64)
RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow

# Project dependencies used by Members 1-3 (subset of requirements.txt;
# numpy/pytest are intentionally left out of the Airflow image)
RUN pip install --no-cache-dir \
    "pyspark>=3.5.0,<4.0" \
    "kafka-python-ng>=2.2.2" \
    "pydantic>=2.0.0" \
    "python-dotenv>=1.0.0" \
    "pyarrow>=14.0.0"
