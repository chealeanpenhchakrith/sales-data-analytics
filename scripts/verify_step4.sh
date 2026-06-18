#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step4.out"

echo "Running Step 4 KPI verification..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv >"${OUTPUT}"

grep -q "=== Step 4: Core KPI Summary ===" "${OUTPUT}"
grep -q "TotalRevenue" "${OUTPUT}"
grep -q "TotalOrders" "${OUTPUT}"
grep -q "TotalQuantitySold" "${OUTPUT}"
grep -q "TotalCustomers" "${OUTPUT}"
grep -q "AverageOrderValue" "${OUTPUT}"
grep -q "CancellationRatePercent" "${OUTPUT}"
grep -q "8911407.9" "${OUTPUT}"
grep -q "18532" "${OUTPUT}"
grep -q "5167812" "${OUTPUT}"
grep -q "4338" "${OUTPUT}"
grep -q "3665" "${OUTPUT}"
grep -q "37" "${OUTPUT}"
grep -q "480.87" "${OUTPUT}"
grep -q "2054.27" "${OUTPUT}"
grep -q "3836" "${OUTPUT}"
grep -q "25900" "${OUTPUT}"
grep -q "14.81" "${OUTPUT}"

echo "Step 4 verification passed."
echo "Output: ${OUTPUT}"
