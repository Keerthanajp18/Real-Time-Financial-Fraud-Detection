FROM apache/airflow:2.7.2-python3.11

# PySpark needs a Java runtime (Member 2 ETL + Member 3 inference + quality checks)
# Java is copied from the official Temurin image: the base image is Debian bullseye,
# whose apt repos are retired, so apt-get install no longer works. Multi-arch (amd64 / arm64).
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Project dependencies used by Members 1-3 (subset of requirements.txt;
# numpy/pytest are intentionally left out of the Airflow image)
RUN pip install --no-cache-dir \
    "pyspark==4.2.0" \
    "kafka-python-ng>=2.2.2" \
    "pydantic>=2.0.0" \
    "python-dotenv>=1.0.0" \
    "pyarrow>=14.0.0,<20"  # pyarrow 20+ needs NumPy 2; Airflow 2.7.2 ships NumPy 1.24
