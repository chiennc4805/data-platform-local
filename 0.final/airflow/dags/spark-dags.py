from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG định nghĩa
with DAG(
    dag_id="spark_etl_to_iceberg",
    description="Submit Spark job ghi dữ liệu vào Iceberg trên MinIO",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "iceberg", "etl"]
) as dag:

    spark_submit_task = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/airflow/jobs/test.py",   # đường dẫn trong container Airflow
        conn_id="spark_default",                        # cấu hình connection trong Airflow UI
        executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
        name="SparkIcebergETL",
        deploy_mode="client",
        conf={
            "spark.sql.catalog.hive": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hive.type": "hive",
            "spark.sql.catalog.hive.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.hive.warehouse": "s3a://warehouse",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio@123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
    )

    spark_submit_task