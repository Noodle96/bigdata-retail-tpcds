from typing import Dict
import sys

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