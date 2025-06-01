from pyspark.sql import SparkSession
import sys
import json
import time
from datetime import datetime

def check_minio_availability(spark):
    """Проверка доступности MinIO"""
    try:
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.s3a.connection.timeout", "5000"
        )
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.s3a.connection.establish.timeout", "5000"
        )
        spark.sql("SELECT 1").collect()
        return True
    except Exception as e:
        print(f"MinIO check failed: {str(e)}", file=sys.stderr)
        return False

def main():
    start_time = time.time()
    if len(sys.argv) < 2:
        print(json.dumps({
            "status": "error",
            "message": "No product update data provided"
        }), file=sys.stderr)
        return 1

    try:
        update_data = json.loads(sys.argv[1])
        required_fields = ["product_id", "product_name", "price", "product_description", "created_at", "updated_at", "category", "seller_id"]
        
        if missing_fields := [f for f in required_fields if f not in update_data]:
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
            .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()

        if not check_minio_availability(spark):
            raise RuntimeError("MinIO is not available")

        if not spark.catalog.tableExists("spark_catalog.db.products"):
            raise RuntimeError("Table products does not exist")

        product_id = update_data["product_id"]
        product_name = update_data["product_name"]
        product_description = update_data["product_description"]
        price = update_data["price"]
        created_at = update_data["created_at"]
        updated_at = update_data["updated_at"]
        category = update_data["category"]
        seller_id = update_data["seller_id"]  

        spark.sql(f"""
            MERGE INTO spark_catalog.db.products target
            USING (
                SELECT
                    '{product_id}' as product_id,
                    '{product_name}' as product_name,
                    '{product_description}' as product_description,
                    '{category}' as category,
                    {price} as price,
                    {seller_id} as seller_id,
                    timestamp('{created_at}') as created_at,
                    timestamp('{updated_at}') as updated_at
            ) source
            ON target.product_id = source.product_id
            WHEN MATCHED THEN UPDATE SET
                target.product_name = source.product_name,
                target.product_description = source.product_description,
                target.category = source.category,
                target.price = source.price,
                target.seller_id = source.seller_id,
                target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                product_id, product_name, product_description, category, price, seller_id, created_at, updated_at
            ) VALUES (
                source.product_id, source.product_name, source.product_description,
                source.category, source.price, source.seller_id,
                source.created_at, source.updated_at
            )
        """)
        
        execution_time = time.time() - start_time
        print(json.dumps({
            "status": "success",
            "execution_time_seconds": execution_time,
            "updated_product_id": product_id
        }))
        return 0

    except Exception as e:
        error_info = {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "timestamp": datetime.now().isoformat()
        }
        print(json.dumps(error_info), file=sys.stderr)
        return 1

    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    sys.exit(main())