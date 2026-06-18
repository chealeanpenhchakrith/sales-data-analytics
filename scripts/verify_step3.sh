#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step3.out"

echo "Running Step 3 data cleaning verification..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv >"${OUTPUT}"

grep -q "=== Step 3: Data Cleaning Summary ===" "${OUTPUT}"
grep -q "Raw rows: 541909" "${OUTPUT}"
grep -q "Rows after cleaning for regular sales analysis: 397884" "${OUTPUT}"
grep -q "Cancelled invoice rows excluded from regular sales: 9288" "${OUTPUT}"
grep -q "Rows with empty Description: 1454" "${OUTPUT}"
grep -q "Rows with empty CustomerID: 135080" "${OUTPUT}"
grep -q "Rows with invalid Quantity: 10624" "${OUTPUT}"
grep -q "Rows with invalid UnitPrice: 2517" "${OUTPUT}"
grep -q "Revenue" "${OUTPUT}"
grep -q "InvoiceDateOnly" "${OUTPUT}"
grep -q "YearMonth" "${OUTPUT}"
grep -q "DayOfWeek" "${OUTPUT}"

echo "Step 3 verification passed."
echo "Output: ${OUTPUT}"
