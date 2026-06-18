#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step5.out"

echo "Running Step 5 sales analysis verification..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv >"${OUTPUT}"

grep -q "=== Step 5: Monthly Sales Trend ===" "${OUTPUT}"
grep -q "=== Step 5: Sales by Day of Week ===" "${OUTPUT}"
grep -q "=== Step 5: Sales by Hour ===" "${OUTPUT}"
grep -q "=== Step 5: Highest and Lowest Revenue Months ===" "${OUTPUT}"
grep -q "2010-12" "${OUTPUT}"
grep -q "2011-12" "${OUTPUT}"
grep -q "HighestRevenueMonth" "${OUTPUT}"
grep -q "2011-11" "${OUTPUT}"
grep -q "1161817.38" "${OUTPUT}"
grep -q "LowestRevenueMonth" "${OUTPUT}"
grep -q "2011-02" "${OUTPUT}"
grep -q "447137.35" "${OUTPUT}"
grep -q "Thursday" "${OUTPUT}"
grep -q "1976859.07" "${OUTPUT}"
grep -q "|12  |1378571.48" "${OUTPUT}"

echo "Step 5 verification passed."
echo "Output: ${OUTPUT}"
