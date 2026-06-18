#!/usr/bin/env bash
set -euo pipefail

echo "Checking Docker..."
docker --version
docker compose version

echo "Checking Docker Compose configuration..."
docker compose config >/dev/null

echo "Checking running services..."
docker compose ps

for service in spark-master spark-worker namenode datanode; do
  if ! docker compose ps --services --filter "status=running" | grep -qx "${service}"; then
    echo "Service is not running: ${service}" >&2
    echo "Start the environment with: docker compose up -d" >&2
    exit 1
  fi
done

echo "Checking HDFS..."
docker compose exec -T namenode hdfs dfs -ls / >/dev/null

echo "Checking Spark submit..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py >/tmp/sales_step1_spark_submit.log

echo "Step 1 verification passed."
echo "Spark UI: http://localhost:8080"
echo "Spark Worker UI: http://localhost:8081"
echo "HDFS NameNode UI: http://localhost:9870"
echo "Spark submit log: /tmp/sales_step1_spark_submit.log"
