from pyspark.sql import SparkSession
import json
import sys

def main():
    spark = SparkSession.builder \
        .appName("ListProducts") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://iceberg/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df = spark.read.format("iceberg").load("iceberg_catalog.db.products")

    products_json = df.toJSON().collect()
    products = [json.loads(p) for p in products_json]

    print(json.dumps(products))

    spark.stop()

    return 0

if __name__ == "__main__":
    sys.exit(main())