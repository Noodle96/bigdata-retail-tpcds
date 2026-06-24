from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestSpark")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("USE retail_tpcds_opt")

df = spark.sql("""
SELECT COUNT(*) AS total
FROM store_sales_opt
""")

df.show()

spark.stop()

# spark-submit test_spark.py