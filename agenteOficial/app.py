import os
import time
import sqlite3
import threading
import psutil
import json
from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
from google import genai

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

# ---------------------------------------------------------------------------
# CONFIG: cambia esto a "remote" cuando pruebes contra Hive/Spark reales
# ---------------------------------------------------------------------------
DB_MODE = os.getenv("DB_MODE", "local")  # "local" -> SQLite | "remote" -> Hive/Spark
SQLITE_PATH = os.getenv("SQLITE_PATH", "tpcds.db")

# Base de datos real del Data Warehouse optimizado (creada en hive/ddl/opt/00_create_optimized_tables.hql)
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "retail_tpcds_opt")

app = Flask(__name__)

ESQUEMA = """
Eres un experto en HiveQL / Spark SQL. Tienes acceso al Data Warehouse optimizado
"retail_tpcds_opt": tablas de hechos en formato Parquet particionadas por (d_year, d_moy)
y tablas de dimensión en Parquet sin particionar. Usa SIEMPRE los nombres de tabla
EXACTOS que se listan abajo (todas terminan en "_opt"):

TABLA customer_opt:
- c_customer_sk (INTEGER, clave primaria)
- c_customer_id (TEXT)
- c_first_name (TEXT)
- c_last_name (TEXT)
- c_birth_year (INTEGER)
- c_email_address (TEXT)

TABLA item_opt:
- i_item_sk (INTEGER, clave primaria)
- i_item_id (TEXT)
- i_product_name (TEXT)
- i_category (TEXT)
- i_current_price (REAL)
- i_brand (TEXT)

TABLA store_opt:
- s_store_sk (INTEGER, clave primaria)
- s_store_id (TEXT)
- s_store_name (TEXT)
- s_city (TEXT)
- s_state (TEXT)
- s_number_employees (INTEGER)

TABLA date_dim_opt:
- d_date_sk (INTEGER, clave primaria)
- d_date_id (TEXT)
- d_date (TEXT)
- d_year (INTEGER)
- d_moy (INTEGER) -- mes del año
- d_dom (INTEGER) -- dia del mes
- d_dow (INTEGER) -- dia de la semana
- d_day_name (TEXT)
- d_quarter_name (TEXT)
Usa esta tabla SOLO si necesitas un atributo de fecha que no esté ya en store_sales_opt
(por ejemplo d_day_name o d_quarter_name). Para filtrar o agrupar solo por año/mes
NO la uses: ver la nota en store_sales_opt.

TABLA store_sales_opt (particionada por d_year, d_moy):
- ss_sold_date_sk (INTEGER) -> relaciona con date_dim_opt.d_date_sk (solo si necesitas
  un atributo de date_dim_opt distinto de d_year/d_moy)
- d_year (INTEGER) -- columna de PARTICION, ya está disponible en esta tabla
- d_moy (INTEGER) -- columna de PARTICION, ya está disponible en esta tabla
- ss_item_sk (INTEGER) -> relaciona con item_opt.i_item_sk
- ss_customer_sk (INTEGER) -> relaciona con customer_opt.c_customer_sk
- ss_store_sk (INTEGER) -> relaciona con store_opt.s_store_sk
- ss_ticket_number (INTEGER)
- ss_quantity (INTEGER)
- ss_sales_price (REAL)
- ss_net_paid (REAL)
- ss_net_profit (REAL)

REGLAS:
1. Responde UNICAMENTE con codigo SQL valido para Hive/Spark SQL.
2. No agregues explicaciones, comentarios, ni texto adicional.
3. No uses la palabra "sql" ni comillas triples (```), solo el SQL puro.
4. Usa los nombres de tabla y columna EXACTAMENTE como estan definidos arriba
   (todas las tablas llevan el sufijo "_opt").
5. Si la consulta solo necesita filtrar o agrupar por año y/o mes, usa las columnas
   d_year / d_moy directamente desde store_sales_opt y NO hagas JOIN con date_dim_opt.
6. Si la pregunta no especifica una cantidad exacta de resultados (ej. "top productos",
"mejores clientes"), usa LIMIT 10 por defecto.
7. Genera siempre SELECT de solo lectura. Nunca generes DROP, DELETE, UPDATE, INSERT,
   ALTER, TRUNCATE ni CREATE.

Ejemplo de pregunta: "Cuantos clientes hay en total?"
Ejemplo de respuesta: SELECT COUNT(*) FROM customer_opt;

Ejemplo de pregunta: "Cuales son los 5 productos mas caros?"
Ejemplo de respuesta: SELECT i_product_name, i_current_price FROM item_opt ORDER BY i_current_price DESC LIMIT 5;

Ejemplo de pregunta: "Que productos generaron mayores ingresos?"
Ejemplo de respuesta: SELECT i.i_product_name, SUM(ss.ss_net_paid) AS total_revenue FROM store_sales_opt ss JOIN item_opt i ON ss.ss_item_sk = i.i_item_sk GROUP BY i.i_product_name ORDER BY total_revenue DESC LIMIT 10;

Ejemplo de pregunta: "Cual fue el mes con mayores ingresos?"
Ejemplo de respuesta: SELECT d_year, d_moy, SUM(ss_net_paid) AS total_ingresos FROM store_sales_opt GROUP BY d_year, d_moy ORDER BY total_ingresos DESC LIMIT 1;
"""

VIZ_INSTRUCCIONES = """
Eres un asistente que decide como visualizar el resultado de una consulta SQL.
Dada la pregunta del usuario, la consulta SQL y las columnas del resultado, responde
UNICAMENTE con un objeto JSON (sin texto adicional, sin comillas triples) con esta forma:

{
  "chart_type": "bar" | "line" | "pie" | "kpi" | "table",
  "x": "nombre_columna_o_null",
  "y": "nombre_columna_o_null",
  "title": "titulo corto para el grafico"
}

Reglas:
- Si el resultado es una sola fila con un solo valor numerico -> "kpi".
- Si hay una columna temporal (mes, dia, anio, fecha) -> "line".
- Si es un ranking/top N de categorias (clientes, productos, tiendas) -> "bar".
- Si son pocas categorias (<=6) representando partes de un total -> "pie".
- Si no se puede graficar de forma clara -> "table".
"""


def generar_sql(pregunta: str) -> str:
    prompt = ESQUEMA + f"\n\nPregunta del usuario: {pregunta}\nSQL:"
    response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
    sql = response.text.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    sql = sql.rstrip(";")
    return sql


def elegir_motor(sql: str) -> str:
    sql_lower = sql.lower()
    cantidad_joins = sql_lower.count("join")
    tiene_group_by = "group by" in sql_lower
    tiene_order_by = "order by" in sql_lower
    tiene_subquery = sql_lower.count("select") > 1

    if cantidad_joins >= 2 or tiene_subquery:
        return "spark"
    if tiene_group_by and tiene_order_by:
        return "spark"
    return "hive"


def generar_metadata_grafico(pregunta: str, sql: str, columnas: list) -> dict:
    prompt = (
        VIZ_INSTRUCCIONES
        + f"\n\nPregunta: {pregunta}\nSQL: {sql}\nColumnas del resultado: {columnas}\nJSON:"
    )
    try:
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        texto = response.text.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(texto)
    except Exception:
        return {"chart_type": "table", "x": None, "y": None, "title": ""}


# ---------------------------------------------------------------------------
# Ejecución de queries + medición de recursos (CPU / memoria) con psutil
# ---------------------------------------------------------------------------

def _medir_durante(func, *args, **kwargs):
    """Ejecuta func midiendo CPU% y memoria pico del proceso actual durante la llamada."""
    proceso = psutil.Process(os.getpid())
    cpu_samples = []
    mem_samples = []
    detener = threading.Event()

    def muestrear():
        proceso.cpu_percent(interval=None)  # primer llamado "reinicia" el contador
        while not detener.is_set():
            cpu_samples.append(proceso.cpu_percent(interval=0.1))
            mem_samples.append(proceso.memory_info().rss / (1024 * 1024))  # MB

    hilo = threading.Thread(target=muestrear)
    hilo.start()

    inicio = time.perf_counter()
    try:
        resultado = func(*args, **kwargs)
    finally:
        fin = time.perf_counter()
        detener.set()
        hilo.join()

    tiempo_ms = (fin - inicio) * 1000
    cpu_max = max(cpu_samples) if cpu_samples else 0.0
    mem_max = max(mem_samples) if mem_samples else proceso.memory_info().rss / (1024 * 1024)

    metricas = {
        "tiempo_ms": round(tiempo_ms, 2),
        "cpu_pct": round(cpu_max, 2),
        "memoria_mb": round(mem_max, 2),
    }
    return resultado, metricas


def ejecutar_local(sql: str):
    # NOTA: el ESQUEMA ahora describe las tablas "*_opt" del warehouse real.
    # Si tpcds.db (SQLite) fue generado con los nombres originales sin sufijo,
    # el SQL generado por el LLM (que usará *_opt) fallará en modo local.
    # Para seguir probando en DB_MODE=local sin el clúster, regenera tpcds.db
    # con las tablas renombradas a *_opt, o crea vistas SQLite equivalentes:
    #   CREATE VIEW customer_opt AS SELECT * FROM customer; (etc.)
    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()
    cursor.execute(sql)
    columnas = [desc[0] for desc in cursor.description]
    resultados = cursor.fetchall()
    conn.close()
    return columnas, resultados


def ejecutar_hive(sql: str):
    from pyhive import hive
    conn = hive.Connection(host="localhost", port=10000, username="hadoop", database=HIVE_DATABASE)
    cursor = conn.cursor()
    cursor.execute(sql)
    columnas = [desc[0] for desc in cursor.description]
    resultados = cursor.fetchall()
    conn.close()
    return columnas, resultados


def ejecutar_spark(sql: str):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("AgenteSQL").enableHiveSupport().getOrCreate()
    spark.sql(f"USE {HIVE_DATABASE}")
    df = spark.sql(sql)
    columnas = df.columns
    resultados = [tuple(r) for r in df.collect()]
    return columnas, resultados


def ejecutar_sql(motor: str, sql: str):
    if DB_MODE == "local":
        # En modo local usamos SQLite para ambos motores (para poder probar el
        # flujo completo sin clúster), pero igual reportamos el motor "decidido".
        return ejecutar_local(sql)
    if motor == "spark":
        return ejecutar_spark(sql)
    return ejecutar_hive(sql)


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html", db_mode=DB_MODE)


@app.route("/api/consultar", methods=["POST"])
def api_consultar():
    data = request.get_json(force=True)
    pregunta = (data.get("pregunta") or "").strip()

    if not pregunta:
        return jsonify({"error": "La pregunta no puede estar vacia."}), 400

    try:
        sql = generar_sql(pregunta)
    except Exception as e:
        return jsonify({"error": f"Error generando SQL: {e}"}), 500

    motor = elegir_motor(sql)

    try:
        (columnas, resultados), metricas = _medir_durante(ejecutar_sql, motor, sql)
    except Exception as e:
        return jsonify({
            "error": f"Error ejecutando la consulta: {e}",
            "sql": sql,
            "motor": motor,
        }), 500

    viz = generar_metadata_grafico(pregunta, sql, columnas)

    # Respuesta en lenguaje natural
    try:
        prompt_respuesta = (
            f'El usuario pregunto: "{pregunta}"\n'
            f"Se ejecuto esta consulta SQL en el motor {motor.upper()}: {sql}\n"
            f"Y se obtuvo este resultado: {resultados}\n\n"
            "Responde la pregunta del usuario en espanol, de forma breve y clara, "
            "usando los datos del resultado. No menciones SQL ni tecnicismos."
        )
        resp = client.models.generate_content(model="gemini-2.5-flash", contents=prompt_respuesta)
        respuesta_nl = resp.text.strip()
    except Exception:
        respuesta_nl = ""

    return jsonify({
        "pregunta": pregunta,
        "sql": sql,
        "motor": motor,
        "metricas": metricas,
        "columnas": columnas,
        "resultados": resultados,
        "visualizacion": viz,
        "respuesta_nl": respuesta_nl,
        "db_mode": DB_MODE,
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)