#!/bin/bash

echo "📦 Инициализация Iceberg-таблицы через Spark..."
/opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hadoop \
  --conf spark.sql.catalog.spark_catalog.warehouse=file:///opt/bitnami/spark/spark-warehouse \
  /app/spark_jobs/init_products_table.py

echo "🚀 Запуск FastAPI-приложения..."
exec "$@"
