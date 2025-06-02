#!/bin/bash
set -e

if [ -f /app/.env ]; then
  export $(grep -v '^#' /app/.env | xargs)
else
  echo ".env файл не найден!"
  exit 1
fi

envsubst < /app/spark/config/spark-defaults.template > /app/spark/config/spark-defaults.conf

echo "✔ spark-defaults.conf сгенерирован"
