#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step7.out"

echo "Running Step 7 customer analysis verification..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv >"${OUTPUT}"

grep -q "=== Step 7: Top 10 High-Value Customers ===" "${OUTPUT}"
grep -q "=== Step 7: Top 10 RFM Customers ===" "${OUTPUT}"
grep -q "=== Step 7: Customer Segment Summary ===" "${OUTPUT}"
grep -q "CustomerRevenue" "${OUTPUT}"
grep -q "CustomerOrderCount" "${OUTPUT}"
grep -q "CustomerQuantityPurchased" "${OUTPUT}"
grep -q "RecencyDays" "${OUTPUT}"
grep -q "CustomerSegment" "${OUTPUT}"
grep -q "|14646     |280206.02" "${OUTPUT}"
grep -q "|18102     |259657.3" "${OUTPUT}"
grep -q "High Value" "${OUTPUT}"
grep -q "|High Value     |201" "${OUTPUT}"
grep -q "|Active         |842" "${OUTPUT}"
grep -q "|Regular        |2434" "${OUTPUT}"
grep -q "|At Risk        |861" "${OUTPUT}"

echo "Step 7 verification passed."
echo "Output: ${OUTPUT}"
