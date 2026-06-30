from typing import Dict, Tuple
import sys

import boto3

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


sys.path.append("/home/hadoop/spark")

from config.tpcds_schemas import (
    TPCDS_SCHEMAS,
    FACT_TABLES,
    DIMENSION_TABLES,
    FACT_DATE_KEYS
)


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

RAW_BASE_PATH: str = "s3://bigdata-russell-academy/data"
WAREHOUSE_BASE_PATH: str = "s3://bigdata-russell-academy/warehouse/tpcds"


# ============================================================
# SPARK SESSION
# ============================================================

def build_spark() -> SparkSession:
    spark: SparkSession = (
        SparkSession.builder
        .appName("TPCDS Warehouse Builder")
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# LECTURA RAW
# ============================================================

def read_raw_table(
    spark: SparkSession,
    table_name: str
) -> DataFrame:

    schema = TPCDS_SCHEMAS[table_name]

    path: str = (
        f"{RAW_BASE_PATH}/{table_name}.dat"
    )

    print(f"Reading: {path}")

    return (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .schema(schema)
        .csv(path)
    )


# ============================================================
# ESCRITURA PARQUET
# ============================================================

def write_parquet(
    df: DataFrame,
    output_path: str
) -> None:

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )


# ============================================================
# ESCRITURA PARTICIONADA
# ============================================================

def write_partitioned_parquet(
    df: DataFrame,
    output_path: str
) -> None:

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(
            "d_year",
            "d_moy"
        )
        .parquet(output_path)
    )


# ============================================================
# LIMPIEZA DE MARCADORES EMRFS ("_$folder$")
# ============================================================
# Al escribir con partitionBy() sobre un path "s3://...", EMRFS (el filesystem
# que usa EMR para el esquema s3://) crea un objeto vacio "<particion>_$folder$"
# por cada directorio de particion que crea (uno por cada d_year=, otro por
# cada d_moy= dentro). Esos objetos quedan DENTRO del LOCATION de la tabla de
# Hive y rompen "MSCK REPAIR TABLE", porque matchean el patron "columna=valor"
# de una particion pero son archivos de 0 bytes, no directorios.
#
# Esta funcion los borra de S3 justo despues de escribir cada tabla de hechos
# particionada, para que la copia hacia HDFS (copy_warehouse_s3_to_hdfs_opt.sh)
# y el posterior MSCK REPAIR TABLE en Hive no se topen con ellos.

def _split_s3_uri(
    s3_uri: str
) -> Tuple[str, str]:

    sin_esquema: str = (
        s3_uri
        .replace("s3://", "")
        .replace("s3a://", "")
    )

    bucket, _, key_prefix = sin_esquema.partition("/")

    return bucket, key_prefix


def clean_folder_markers(
    s3_uri: str
) -> None:

    bucket, prefix = _split_s3_uri(s3_uri)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    objetos_a_borrar = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("_$folder$"):
                objetos_a_borrar.append({"Key": obj["Key"]})

    if not objetos_a_borrar:
        print(f"  Sin marcadores _$folder$ en {s3_uri}")
        return

    print(f"  Borrando {len(objetos_a_borrar)} marcadores _$folder$ en {s3_uri}")

    # delete_objects acepta maximo 1000 keys por llamada
    for i in range(0, len(objetos_a_borrar), 1000):
        lote = objetos_a_borrar[i:i + 1000]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": lote})


# ============================================================
# DIMENSIONES
# ============================================================

def process_dimension_tables(
    spark: SparkSession
) -> None:

    print("\n==============================")
    print("PROCESSING DIMENSIONS")
    print("==============================\n")

    for table_name in DIMENSION_TABLES:

        print(f"Dimension: {table_name}")

        df: DataFrame = read_raw_table(
            spark,
            table_name
        )

        output_path: str = (
            f"{WAREHOUSE_BASE_PATH}/{table_name}"
        )

        write_parquet(
            df,
            output_path
        )


# ============================================================
# DATE DIM
# ============================================================

def load_date_dim(
    spark: SparkSession
) -> DataFrame:

    date_dim: DataFrame = read_raw_table(
        spark,
        "date_dim"
    )

    return date_dim.select(
        "d_date_sk",
        "d_year",
        "d_moy"
    )


# ============================================================
# ENRIQUECER HECHOS
# ============================================================

def enrich_fact_table(
    fact_df: DataFrame,
    fact_table: str,
    date_dim: DataFrame
) -> DataFrame:

    date_key: str = FACT_DATE_KEYS[fact_table]

    enriched_df: DataFrame = (
        fact_df.join(
            date_dim,
            fact_df[date_key] == date_dim["d_date_sk"],
            "left"
        )
        .drop("d_date_sk")
    )

    return enriched_df


# ============================================================
# HECHOS
# ============================================================

def process_fact_tables(
    spark: SparkSession
) -> None:

    print("\n==============================")
    print("PROCESSING FACT TABLES")
    print("==============================\n")

    date_dim: DataFrame = load_date_dim(
        spark
    )

    for table_name in FACT_TABLES:

        print(f"Fact: {table_name}")

        fact_df: DataFrame = read_raw_table(
            spark,
            table_name
        )

        fact_df = enrich_fact_table(
            fact_df,
            table_name,
            date_dim
        )

        output_path: str = (
            f"{WAREHOUSE_BASE_PATH}/{table_name}"
        )

        write_partitioned_parquet(
            fact_df,
            output_path
        )

        clean_folder_markers(
            output_path
        )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    spark: SparkSession = build_spark()

    process_dimension_tables(
        spark
    )

    process_fact_tables(
        spark
    )

    spark.stop()


if __name__ == "__main__":
    main()