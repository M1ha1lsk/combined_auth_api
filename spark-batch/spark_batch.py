import logging
from clickhouse_driver import Client
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max, lit, col, current_timestamp
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder \
    .appName("spark-batch") \
    .config(
        "spark.jars",
        "/opt/spark-apps/jars/postgresql-42.6.0.jar,/opt/spark-apps/jars/clickhouse-jdbc-0.4.6.jar"
    ) \
    .getOrCreate()

postgres_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
postgres_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
clickhouse_properties = {
    "user": os.getenv("CLICKHOUSE_USER"),
    "password": os.getenv("CLICKHOUSE_PASSWORD"),
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

def optimize_clickhouse_table(table_name):
    logging.info(f"Optimizing ClickHouse table {table_name}...")
    client = Client(
        host='clickhouse',
        user=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database='default'
    )
    client.execute(f"OPTIMIZE TABLE {table_name} FINAL")
    logging.info(f"OPTIMIZE TABLE FINAL executed for {table_name}")


def sync_table(table_name, id_col, batch_time):
    logging.info(f"Start syncing table: {table_name}")
    ch_df = spark.read.jdbc(clickhouse_url, table_name, properties=clickhouse_properties)
    last_batch_time = ch_df.agg(spark_max("batch_time")).collect()[0][0]
    if last_batch_time is None:
        logging.info("No previous batch_time found. Defaulting to 1970-01-01.")
        last_batch_time = "1970-01-01 00:00:00"
    logging.info(f"Last batch_time: {last_batch_time}")

    query = f"(SELECT * FROM {table_name} WHERE updated_at > TIMESTAMP '{last_batch_time}') AS src"
    pg_df = spark.read.jdbc(postgres_url, query, properties=postgres_properties)
    if pg_df.rdd.isEmpty():
        logging.info(f"No new records to sync for {table_name}.")
    else:
        enriched_df = pg_df.withColumn("batch_time", lit(batch_time)) \
                           .withColumn("is_deleted", lit(False))
        enriched_df.write \
            .mode("append") \
            .jdbc(clickhouse_url, table_name, properties=clickhouse_properties)
        logging.info(f"Inserted {enriched_df.count()} records into {table_name}.")

    logging.info(f"Checking for deleted records in {table_name}")

    ch_ids_df = ch_df.select(id_col).distinct()
    pg_all_df = spark.read.jdbc(postgres_url, table_name, properties=postgres_properties)
    pg_ids_df = pg_all_df.select(id_col).distinct()

    deleted_ids_df = ch_ids_df.join(pg_ids_df, id_col, "left_anti")
    if deleted_ids_df.rdd.isEmpty():
        logging.info(f"No deleted records found for {table_name}.")
    else:
        deleted_records_df = ch_df.join(deleted_ids_df, id_col).dropDuplicates([id_col]) \
            .withColumn("is_deleted", lit(True)) \
            .withColumn("batch_time", lit(batch_time)) \
            .withColumn("updated_at", current_timestamp())
        deleted_records_df.write \
            .mode("append") \
            .jdbc(clickhouse_url, table_name, properties=clickhouse_properties)
        logging.info(f"Marked {deleted_records_df.count()} records as deleted in {table_name}.")

    logging.info(f"Finished syncing table: {table_name} ---\n")


if __name__ == "__main__":
    current_batch_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Using common batch_time: {current_batch_time}")

    sync_table("customers", "customer_id", current_batch_time)
    sync_table("sellers",   "seller_id",   current_batch_time)
    spark.stop()
    logging.info("Spark session stopped")
    optimize_clickhouse_table("sellers")
    optimize_clickhouse_table