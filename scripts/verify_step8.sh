#!/usr/bin/env bash
set -euo pipefail

OUTPUT="/tmp/sales_step8.out"

echo "Running Step 8 country analysis verification..."
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/script.py hdfs://namenode:9000/data/online-retail/online_retail.csv >"${OUTPUT}"

grep -q "=== Step 8: Top 10 Countries by Revenue ===" "${OUTPUT}"
grep -q "=== Step 8: UK vs Non-UK Market Share ===" "${OUTPUT}"
grep -q "=== Step 8: Potential Growth Markets ===" "${OUTPUT}"
grep -q "CountryRevenue" "${OUTPUT}"
grep -q "CountryOrderCount" "${OUTPUT}"
grep -q "CountryCustomerCount" "${OUTPUT}"
grep -q "CountryQuantitySold" "${OUTPUT}"
grep -q "|United Kingdom|7308391.55" "${OUTPUT}"
grep -q "|Netherlands   |285446.34" "${OUTPUT}"
grep -q "|EIRE          |265545.9" "${OUTPUT}"
grep -q "|United Kingdom|7308391.55   |16646" "${OUTPUT}"
grep -q "|Non-UK        |1603016.35   |1886" "${OUTPUT}"
grep -q "82.01" "${OUTPUT}"
grep -q "17.99" "${OUTPUT}"
grep -q "|Austria        |10198.68" "${OUTPUT}"
grep -q "|Poland         |7334.65" "${OUTPUT}"

echo "Step 8 verification passed."
echo "Output: ${OUTPUT}"
