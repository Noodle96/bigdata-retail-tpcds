from pyhive import hive

conn = hive.Connection(
    host="localhost",
    port=10000,
    username="hadoop",
    database="retail_tpcds_opt"
)

cursor = conn.cursor()

cursor.execute("""
SELECT COUNT(*)
FROM store_sales_opt
""")

resultado = cursor.fetchall()

print("Resultado:")
print(resultado)

cursor.close()
conn.close()

# python test_hive.py