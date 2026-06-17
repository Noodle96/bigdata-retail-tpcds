#!/bin/bash

set -e

S3_BASE="s3://bigdata-russell-academy/data"
HDFS_BASE="hdfs:///datasets/tpcds/raw"

hdfs dfs -mkdir -p /datasets/tpcds/raw/customer
hdfs dfs -mkdir -p /datasets/tpcds/raw/item
hdfs dfs -mkdir -p /datasets/tpcds/raw/store
hdfs dfs -mkdir -p /datasets/tpcds/raw/date_dim
hdfs dfs -mkdir -p /datasets/tpcds/raw/store_sales

s3-dist-cp --src "${S3_BASE}/customer.dat" --dest "${HDFS_BASE}/customer/"
s3-dist-cp --src "${S3_BASE}/item.dat" --dest "${HDFS_BASE}/item/"
s3-dist-cp --src "${S3_BASE}/store.dat" --dest "${HDFS_BASE}/store/"
s3-dist-cp --src "${S3_BASE}/date_dim.dat" --dest "${HDFS_BASE}/date_dim/"
s3-dist-cp --src "${S3_BASE}/store_sales.dat" --dest "${HDFS_BASE}/store_sales/"

hdfs dfs -ls -h /datasets/tpcds/raw/customer
hdfs dfs -ls -h /datasets/tpcds/raw/item
hdfs dfs -ls -h /datasets/tpcds/raw/store
hdfs dfs -ls -h /datasets/tpcds/raw/date_dim
hdfs dfs -ls -h /datasets/tpcds/raw/store_sales