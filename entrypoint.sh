#!/bin/bash
# custom-entrypoint.sh

echo "🔧 Running Iceberg init script..."
/opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  /app/spark_jobs/init_products_table.py

echo "🚀 Starting Spark..."
exec /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh
