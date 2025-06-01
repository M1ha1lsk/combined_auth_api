from pyspark.sql import SparkSession
import sys
import json

def main():
    if len(sys.argv) < 2:
        print("No product update data provided", file=sys.stderr)
        sys.exit(1)
        return 1

    try:
        update_data = json.loads(sys.argv[1])

        required_fields = ["product_id", "product_name", "price"]
        missing_fields = [f for f in required_fields if f not in update_data]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        spark = SparkSession.builder \
            .appName("UpdateProduct") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        timestamp = spark.sql("SELECT current_timestamp()").collect()[0][0]
        update_data["updated_at"] = str(timestamp)
        if "created_at" not in update_data:
            update_data["created_at"] = update_data["updated_at"]

        update_df = spark.createDataFrame([update_data])
        update_df.createOrReplaceTempView("updates")

        spark.sql("""
            MERGE INTO spark_catalog.db.products AS target
            USING updates AS source
            ON target.product_id = source.product_id
            WHEN MATCHED THEN UPDATE SET
                target.product_name = source.product_name,
                target.price = source.price,
                target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                product_id, product_name, price, created_at, updated_at
            ) VALUES (
                source.product_id, source.product_name, source.price, source.created_at, source.updated_at
            )
        """)

        print("Product successfully updated")
        return 0

    except Exception as e:
        error_info = {
            "error": str(e),
            "type": type(e).__name__,
            "args": e.args if hasattr(e, 'args') else None
        }
        print(json.dumps({"status": "error", "details": error_info}), file=sys.stderr)
        sys.exit(1)
        return 1

    finally:
        if 'spark' in locals():
            spark.stop()
        
if __name__ == "__main__":
    sys.exit(main())