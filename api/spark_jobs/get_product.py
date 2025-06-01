from pyspark.sql import SparkSession
import sys
import json
from datetime import datetime

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Product ID not provided"}), file=sys.stderr)
        sys.exit(1)
        
    product_id = sys.argv[1]
    
    spark = SparkSession.builder \
        .appName("GetProduct") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.sources.bucketing.enabled", "true") \
        .getOrCreate()

    try:
        if not spark.catalog.tableExists("spark_catalog.db.products"):
            print(json.dumps({"status": "error", "message": "Table products does not exist"}), file=sys.stderr)
            sys.exit(1)

        start_time = datetime.now()
        print(f"Starting query at {start_time}", file=sys.stderr)

        product_df = spark.table("spark_catalog.db.products") \
            .filter(f"product_id = '{product_id}'") \
            .limit(1)

        if product_df.rdd.isEmpty():
            print(json.dumps({"status": "error", "message": "Product not found"}), file=sys.stderr)
            sys.exit(0)
            
        product = {k: v for k, v in product_df.first().asDict().items() 
                 if not k.startswith('_')}
        
        print(json.dumps({
            "status": "success",
            "product": product,
            "query_time_seconds": (datetime.now() - start_time).total_seconds()
        }))
        
    except Exception as e:
        print(json.dumps({
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }), file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session closed", file=sys.stderr)

if __name__ == "__main__":
    main()