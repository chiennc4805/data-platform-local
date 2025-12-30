FROM apache/airflow:2.7.3-python3.8

USER root

# ⚙️ Thêm repo chuẩn cho Debian và cài OpenJDK 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk 

# ⚙️ Set biến môi trường cho Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# ⚙️ Cài Spark provider và PySpark
RUN pip install --no-cache-dir pyspark==3.5.6 apache-airflow-providers-apache-spark==4.8.0