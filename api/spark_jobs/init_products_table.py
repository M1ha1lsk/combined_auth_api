from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = SparkSession.builder \
    .appName("CreateIcebergTable") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), False),
    StructField("stock", IntegerType(), False)
])

data = [
    ("p001", "iPhone 14", "Electronics", 999.99, 100),
    ("p002", "Samsung Galaxy S22", "Electronics", 899.99, 150),
    ("p003", "MacBook Pro", "Computers", 1999.99, 50),
]

df = spark.createDataFrame(data, schema)
df.writeTo("spark_catalog.db.products").using("iceberg").createOrReplace()
