#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step6.out"

echo "Running Step 6 product analysis verification..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv >"${OUTPUT}"

grep -q "=== Step 6: Top 10 Best-Selling Products by Quantity ===" "${OUTPUT}"
grep -q "=== Step 6: Top 10 Products by Revenue ===" "${OUTPUT}"
grep -q "=== Step 6: Bottom 10 Products by Revenue ===" "${OUTPUT}"
grep -q "ProductQuantitySold" "${OUTPUT}"
grep -q "ProductRevenue" "${OUTPUT}"
grep -q "ProductOrderCount" "${OUTPUT}"
grep -q "PAPER CRAFT , LITTLE BIRDIE" "${OUTPUT}"
grep -q "80995" "${OUTPUT}"
grep -q "168469.6" "${OUTPUT}"
grep -q "REGENCY CAKESTAND 3 TIER" "${OUTPUT}"
grep -q "142592.95" "${OUTPUT}"
grep -q "PADS TO MATCH ALL CUSHIONS" "${OUTPUT}"
grep -q "|PADS" "${OUTPUT}"

echo "Step 6 verification passed."
echo "Output: ${OUTPUT}"
