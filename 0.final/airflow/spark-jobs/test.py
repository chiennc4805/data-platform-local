from pyspark.sql import SparkSession

# Tạo SparkSession với Iceberg + Hive Metastore
spark = (
    SparkSession.builder
    .appName("WriteIcebergToMinIO")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hive")
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")
    # override default warehouse để tránh Spark dùng local
    .config("spark.sql.warehouse.dir", "s3a://warehouse/")
    # MinIO configs
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio@123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

hadoop_conf = spark._jsc.hadoopConfiguration()
print(hadoop_conf.get("fs.s3a.impl"))


# Tạo DataFrame với 5 dòng dữ liệu
data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David"),
    (5, "Eva")
]
df = spark.createDataFrame(data, ["id", "name"])

# Ghi vào bảng Iceberg trong schema default
df.writeTo("iceberg.default.people") \
  .using("iceberg") \
  .createOrReplace()

print("✅ Đã ghi bảng Iceberg: iceberg.default.people")

spark.stop()