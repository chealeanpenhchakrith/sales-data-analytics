#!/usr/bin/env bash
set -euo pipefail

LOCAL_OUTPUT="/tmp/sales_step2_local.out"
HDFS_OUTPUT="/tmp/sales_step2_hdfs.out"
HDFS_PATH="hdfs://namenode:9000/data/online-retail/online_retail.csv"

echo "Running Step 2 with local container path..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py >"${LOCAL_OUTPUT}"

echo "Running Step 2 with HDFS path..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py "${HDFS_PATH}" >"${HDFS_OUTPUT}"

grep -q "=== Step 2: Raw Schema ===" "${LOCAL_OUTPUT}"
grep -q "=== Step 2: Raw Row Count ===" "${LOCAL_OUTPUT}"
grep -q "541909" "${LOCAL_OUTPUT}"
grep -q "=== Step 2: Raw Null or Empty Value Count by Column ===" "${LOCAL_OUTPUT}"
grep -q "=== Step 2: Raw Sample Data ===" "${LOCAL_OUTPUT}"

grep -q "${HDFS_PATH}" "${HDFS_OUTPUT}"
grep -q "541909" "${HDFS_OUTPUT}"

echo "Step 2 verification passed."
echo "Local output: ${LOCAL_OUTPUT}"
echo "HDFS output: ${HDFS_OUTPUT}"
