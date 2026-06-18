#!/usr/bin/env bash
set -euo pipefail

HDFS_DIR="/data/online-retail"
HDFS_FILE="${HDFS_DIR}/online_retail.csv"
LOCAL_FILE="/upload/online_retail.csv"

docker compose exec -T namenode hdfs dfs -mkdir -p "${HDFS_DIR}"
docker compose exec -T namenode hdfs dfs -put -f "${LOCAL_FILE}" "${HDFS_FILE}"
docker compose exec -T namenode hdfs dfs -ls -h "${HDFS_DIR}"

echo "Uploaded dataset to hdfs://namenode:9000${HDFS_FILE}"
