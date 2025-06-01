from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp
import sys

def main():
    try:
        spark = SparkSession.builder \
            .appName("CreateIcebergTable") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        spark.sql("DROP TABLE IF EXISTS spark_catalog.db.products")

        data = [
            ("550e8400-e29b-41d4-a716-446655440000", "iPhone 14", "Latest iPhone model", "Electronics", 999.99, 1),
            ("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "MacBook Pro", "Apple laptop", "Computers", 1999.99, 1),
            ("6ba7b811-9dad-11d1-80b4-00c04fd430c8", "AirPods Pro", "Wireless earbuds", "Accessories", 249.99, 2),
        ]

        # Создаем DataFrame
        df = spark.createDataFrame(data, [
            "product_id", "product_name", "product_description", "category", "price", "seller_id"
        ])

        current_time = current_timestamp()
        df = df.withColumn("created_at", current_time) \
               .withColumn("updated_at", current_time)

        df.writeTo("spark_catalog.db.products") \
          .using("iceberg") \
          .partitionedBy("category") \
          .createOrReplace()

        print("Таблица products успешно создана в Iceberg!")
        return 0
        
    except Exception as e:
        print(f"Ошибка: {str(e)}", file=sys.stderr)
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())