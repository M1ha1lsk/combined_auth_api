from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
import traceback


def main():
    env_path = Path(__file__).resolve().parents[2] / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        minio_user = os.getenv("MINIO_ROOT_USER", "admin")
        minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")
        bucket_name = os.getenv("MINIO_BUCKET_NAME")

        spark = SparkSession.builder \
            .appName("CreateEmptyIcebergTable") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db")

        spark.sql("DROP TABLE IF EXISTS spark_catalog.db.products")

        schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("product_description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", FloatType(), False),
            StructField("seller_id", IntegerType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

        empty_df.writeTo("spark_catalog.db.products") \
                .using("iceberg") \
                .partitionedBy("category") \
                .createOrReplace()

        print("Пустая таблица products успешно создана в Iceberg!")
        return 0

    except Exception as e:
        print(f"Ошибка: {str(e)}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    sys.exit(main())
