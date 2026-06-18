#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step9.out"
HDFS_DIR="/data/online-retail"
HDFS_FILE="${HDFS_DIR}/online_retail.csv"
HDFS_PATH="hdfs://namenode:9000${HDFS_FILE}"

echo "Running Step 9 HDFS integration verification..."

docker compose up -d namenode datanode spark-master spark-worker

docker compose exec -T namenode hdfs dfs -mkdir -p "${HDFS_DIR}"
docker compose exec -T namenode hdfs dfs -put -f /upload/online_retail.csv "${HDFS_FILE}"
docker compose exec -T namenode hdfs dfs -test -e "${HDFS_FILE}"
docker compose exec -T namenode hdfs dfs -ls -h "${HDFS_DIR}"

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py "${HDFS_PATH}" >"${OUTPUT}"

grep -q "${HDFS_PATH}" "${OUTPUT}"
grep -q "=== Step 2: Raw Row Count ===" "${OUTPUT}"
grep -q "541909" "${OUTPUT}"
grep -q "=== Step 8: Country Analysis" "${OUTPUT}" || grep -q "=== Step 8: Top 10 Countries by Revenue ===" "${OUTPUT}"
grep -q "United Kingdom" "${OUTPUT}"

echo "Step 9 verification passed."
echo "HDFS dataset: ${HDFS_PATH}"
echo "Output: ${OUTPUT}"
