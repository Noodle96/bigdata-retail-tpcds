#!/bin/bash

set -e

S3_BASE="s3://bigdata-russell-academy/data"
HDFS_BASE="/datasets/tpcds/raw"

TABLES=(
  call_center
  catalog_page
  catalog_returns
  catalog_sales
  customer
  customer_address
  customer_demographics
  date_dim
  household_demographics
  income_band
  inventory
  item
  promotion
  reason
  ship_mode
  store
  store_returns
  store_sales
  time_dim
  warehouse
  web_page
  web_returns
  web_sales
  web_site
)

echo "============================================================"
echo "Copiando tablas TPC-DS desde S3 hacia HDFS"
echo "Origen:  ${S3_BASE}"
echo "Destino: ${HDFS_BASE}"
echo "============================================================"

hdfs dfs -mkdir -p "${HDFS_BASE}"

for TABLE in "${TABLES[@]}"; do
  echo ""
  echo "------------------------------------------------------------"
  echo "Procesando tabla: ${TABLE}"
  echo "------------------------------------------------------------"

  hdfs dfs -mkdir -p "${HDFS_BASE}/${TABLE}"

  echo "Limpiando archivos previos en HDFS..."
  hdfs dfs -rm -f "${HDFS_BASE}/${TABLE}/${TABLE}.dat" || true
  hdfs dfs -rm -f "${HDFS_BASE}/${TABLE}/part-*" || true
  hdfs dfs -rm -f "${HDFS_BASE}/${TABLE}/_*" || true

  echo "Copiando ${TABLE}.dat..."
  s3-dist-cp \
    --src "${S3_BASE}/${TABLE}.dat" \
    --dest "hdfs://${HDFS_BASE}/${TABLE}/"

  echo "Contenido HDFS:"
  hdfs dfs -ls -h "${HDFS_BASE}/${TABLE}"
done

echo ""
echo "============================================================"
echo "Copia finalizada."
echo "Resumen de carpetas en HDFS:"
echo "============================================================"

hdfs dfs -du -h "${HDFS_BASE}"