#!/usr/bin/env bash
set -euo pipefail

RUN_OUTPUT="/tmp/sales_step10.out"
HDFS_DIR="/data/online-retail"
HDFS_FILE="${HDFS_DIR}/online_retail.csv"
HDFS_PATH="hdfs://namenode:9000${HDFS_FILE}"
LOCAL_OUTPUT_DIR="output"
CONTAINER_OUTPUT_DIR="/app/${LOCAL_OUTPUT_DIR}"

echo "Running Step 10 output verification..."

mkdir -p "${LOCAL_OUTPUT_DIR}"
chmod 777 "${LOCAL_OUTPUT_DIR}"

docker compose up -d namenode datanode spark-master spark-worker

docker compose exec -T namenode hdfs dfs -mkdir -p "${HDFS_DIR}"
docker compose exec -T namenode hdfs dfs -put -f /upload/online_retail.csv "${HDFS_FILE}"

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py "${HDFS_PATH}" "${CONTAINER_OUTPUT_DIR}" >"${RUN_OUTPUT}"

grep -q "=== Step 10: Export Analysis Results ===" "${RUN_OUTPUT}"
grep -q "Exported overall_kpis" "${RUN_OUTPUT}"
grep -q "Exported monthly_sales_trend" "${RUN_OUTPUT}"
grep -q "Exported top_products_by_revenue" "${RUN_OUTPUT}"
grep -q "Exported top_customers" "${RUN_OUTPUT}"
grep -q "Exported country_performance" "${RUN_OUTPUT}"

test -f "${LOCAL_OUTPUT_DIR}/overall_kpis/overall_kpis.csv"
test -f "${LOCAL_OUTPUT_DIR}/overall_kpis/overall_kpis.json"
test -f "${LOCAL_OUTPUT_DIR}/monthly_sales_trend/monthly_sales_trend.csv"
test -f "${LOCAL_OUTPUT_DIR}/top_products_by_revenue/top_products_by_revenue.csv"
test -f "${LOCAL_OUTPUT_DIR}/top_customers/top_customers.csv"
test -f "${LOCAL_OUTPUT_DIR}/country_performance/country_performance.csv"
test -f "${LOCAL_OUTPUT_DIR}/market_share_uk_vs_non_uk/market_share_uk_vs_non_uk.json"

grep -q "8911407.9" "${LOCAL_OUTPUT_DIR}/overall_kpis/overall_kpis.csv"
grep -q "2011-11" "${LOCAL_OUTPUT_DIR}/monthly_sales_trend/monthly_sales_trend.csv"
grep -q "PAPER CRAFT" "${LOCAL_OUTPUT_DIR}/top_products_by_revenue/top_products_by_revenue.csv"
grep -q "14646" "${LOCAL_OUTPUT_DIR}/top_customers/top_customers.csv"
grep -q "United Kingdom" "${LOCAL_OUTPUT_DIR}/country_performance/country_performance.csv"

echo "Step 10 verification passed."
echo "Run output: ${RUN_OUTPUT}"
echo "Analysis tables: ${LOCAL_OUTPUT_DIR}/"
