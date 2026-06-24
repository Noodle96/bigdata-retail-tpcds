#!/bin/bash

set -e

S3_WAREHOUSE="s3://bigdata-russell-academy/warehouse/tpcds"
HDFS_OPT="/warehouse/tpcds/opt"

echo "Limpiando destino HDFS..."
hdfs dfs -rm -r -f "${HDFS_OPT}" || true
hdfs dfs -mkdir -p "${HDFS_OPT}"

echo "Copiando Warehouse Parquet desde S3 hacia HDFS..."
s3-dist-cp \
  --src "${S3_WAREHOUSE}/" \
  --dest "hdfs://${HDFS_OPT}/"

echo "Validando copia..."
hdfs dfs -du -h "${HDFS_OPT}"
hdfs dfs -ls "${HDFS_OPT}"

echo "Copia finalizada correctamente."