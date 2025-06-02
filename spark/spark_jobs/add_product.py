from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import sys
import json
import traceback
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

try:
    env_path = Path(__file__).resolve().parents[2] / '.env'
    load_dotenv(dotenv_path=env_path)
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    bucket_name = os.getenv("MINIO_BUCKET_NAME", "iceberg")
    
    if len(sys.argv) < 2:
        print("ERROR: Необходимо передать JSON с данными продукта", file=sys.stderr)
        sys.exit(1)
        
    product_data = json.loads(sys.argv[1])
    
    spark = SparkSession.builder \
        .appName("AddProduct") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    if not spark.catalog.tableExists("spark_catalog.db.products"):
        print("ERROR: Таблица products не существует", file=sys.stderr)
        sys.exit(1)

    schema = StructType([
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("product_description", StringType(), nullable=True),
        StructField("category", StringType(), nullable=False),
        StructField("price", FloatType(), nullable=False),
        StructField("seller_id", IntegerType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False)
    ])

    current_time = datetime.now()

    new_product = spark.createDataFrame([(
        product_data["product_id"],
        product_data["product_name"],
        product_data.get("product_description", ""),
        product_data["category"],
        float(product_data["price"]),
        int(product_data["seller_id"]),
        current_time,
        current_time
    )], schema=schema)

    new_product.writeTo("spark_catalog.db.products").append()
    
    result = {
        "status": "success",
        "product_id": product_data["product_id"],
        "message": "Product added successfully"
    }
    print(json.dumps(result))
    
except Exception as e:
    error = {
        "status": "error",
        "message": str(e),
        "traceback": traceback.format_exc()
    }
    print(json.dumps(error), file=sys.stderr)
    sys.exit(1)
finally:
    spark.stop()