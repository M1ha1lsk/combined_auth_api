from pyspark.sql import SparkSession
import json
import os

spark = SparkSession.builder \
    .appName("ListProducts") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", f"s3a://{os.getenv('MINIO_BUCKET_NAME')}/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ROOT_USER')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_ROOT_PASSWORD')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df = spark.read.format("iceberg").load("iceberg_catalog.db.products")

products_json = df.toJSON().collect()
products = [json.loads(p) for p in products_json]

print(json.dumps(products))

spark.stop()
