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
DB_MODE = os.getenv("DB_MODE", "remote")  # "local" -> SQLite | "remote" -> Hive/Spark
SQLITE_PATH = os.getenv("SQLITE_PATH", "tpcds.db")

# Base de datos real del Data Warehouse optimizado (creada en hive/ddl/opt/00_create_optimized_tables.hql)
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "retail_tpcds_opt")

app = Flask(__name__)

ESQUEMA = """
Eres un experto en HiveQL / Spark SQL. Tienes acceso al Data Warehouse optimizado
"retail_tpcds_opt" (base de datos Hive), con 24 tablas en formato Parquet:
7 tablas de hechos particionadas por (d_year, d_moy) y 17 tablas de dimensión
sin particionar. Usa SIEMPRE los nombres de tabla EXACTOS de abajo (todas
terminan en "_opt").

============================================================
TABLAS DE HECHOS (particionadas por d_year, d_moy)
============================================================
Estas 7 tablas YA tienen las columnas d_year y d_moy disponibles directamente
(son columnas de partición). Si una consulta solo necesita filtrar/agrupar por
año o mes, usa esas columnas directamente desde la tabla de hechos y NO hagas
JOIN con date_dim_opt. Usa date_dim_opt unicamente cuando necesites un atributo
que no sea d_year/d_moy (dia de la semana, nombre del dia, trimestre, feriado).

TABLA store_sales_opt:
- ss_sold_date_sk (INTEGER) -> date_dim_opt.d_date_sk (solo si necesitas un
  atributo de date_dim_opt distinto de d_year/d_moy)
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- ss_item_sk (INTEGER) -> item_opt.i_item_sk
- ss_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- ss_cdemo_sk (INTEGER) -> customer_demographics_opt.cd_demo_sk
- ss_hdemo_sk (INTEGER) -> household_demographics_opt.hd_demo_sk
- ss_addr_sk (INTEGER) -> customer_address_opt.ca_address_sk
- ss_store_sk (INTEGER) -> store_opt.s_store_sk
- ss_promo_sk (INTEGER) -> promotion_opt.p_promo_sk
- ss_ticket_number (INTEGER)
- ss_quantity (INTEGER)
- ss_wholesale_cost, ss_list_price, ss_sales_price (DECIMAL)
- ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt (DECIMAL)
- ss_net_paid, ss_net_paid_inc_tax, ss_net_profit (DECIMAL)

TABLA catalog_sales_opt:
- cs_sold_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- cs_ship_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- cs_bill_customer_sk, cs_ship_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- cs_bill_cdemo_sk, cs_ship_cdemo_sk (INTEGER) -> customer_demographics_opt.cd_demo_sk
- cs_bill_hdemo_sk, cs_ship_hdemo_sk (INTEGER) -> household_demographics_opt.hd_demo_sk
- cs_bill_addr_sk, cs_ship_addr_sk (INTEGER) -> customer_address_opt.ca_address_sk
- cs_call_center_sk (INTEGER) -> call_center_opt.cc_call_center_sk
- cs_catalog_page_sk (INTEGER) -> catalog_page_opt.cp_catalog_page_sk
- cs_ship_mode_sk (INTEGER) -> ship_mode_opt.sm_ship_mode_sk
- cs_warehouse_sk (INTEGER) -> warehouse_opt.w_warehouse_sk
- cs_item_sk (INTEGER) -> item_opt.i_item_sk
- cs_promo_sk (INTEGER) -> promotion_opt.p_promo_sk
- cs_order_number (INTEGER)
- cs_quantity (INTEGER)
- cs_wholesale_cost, cs_list_price, cs_sales_price (DECIMAL)
- cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost (DECIMAL)
- cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit (DECIMAL)

TABLA web_sales_opt:
- ws_sold_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- ws_ship_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- ws_item_sk (INTEGER) -> item_opt.i_item_sk
- ws_bill_customer_sk, ws_ship_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- ws_bill_cdemo_sk, ws_ship_cdemo_sk (INTEGER) -> customer_demographics_opt.cd_demo_sk
- ws_bill_hdemo_sk, ws_ship_hdemo_sk (INTEGER) -> household_demographics_opt.hd_demo_sk
- ws_bill_addr_sk, ws_ship_addr_sk (INTEGER) -> customer_address_opt.ca_address_sk
- ws_web_page_sk (INTEGER) -> web_page_opt.wp_web_page_sk
- ws_web_site_sk (INTEGER) -> web_site_opt.web_site_sk
- ws_ship_mode_sk (INTEGER) -> ship_mode_opt.sm_ship_mode_sk
- ws_warehouse_sk (INTEGER) -> warehouse_opt.w_warehouse_sk
- ws_promo_sk (INTEGER) -> promotion_opt.p_promo_sk
- ws_order_number (INTEGER)
- ws_quantity (INTEGER)
- ws_wholesale_cost, ws_list_price, ws_sales_price (DECIMAL)
- ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost (DECIMAL)
- ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit (DECIMAL)

TABLA store_returns_opt:
- sr_returned_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- sr_item_sk (INTEGER) -> item_opt.i_item_sk
- sr_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- sr_cdemo_sk (INTEGER) -> customer_demographics_opt.cd_demo_sk
- sr_hdemo_sk (INTEGER) -> household_demographics_opt.hd_demo_sk
- sr_addr_sk (INTEGER) -> customer_address_opt.ca_address_sk
- sr_store_sk (INTEGER) -> store_opt.s_store_sk
- sr_reason_sk (INTEGER) -> reason_opt.r_reason_sk
- sr_ticket_number (INTEGER)
- sr_return_quantity (INTEGER)
- sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost (DECIMAL)
- sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss (DECIMAL)

TABLA catalog_returns_opt:
- cr_returned_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- cr_item_sk (INTEGER) -> item_opt.i_item_sk
- cr_refunded_customer_sk, cr_returning_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- cr_refunded_cdemo_sk, cr_returning_cdemo_sk (INTEGER) -> customer_demographics_opt.cd_demo_sk
- cr_refunded_hdemo_sk, cr_returning_hdemo_sk (INTEGER) -> household_demographics_opt.hd_demo_sk
- cr_refunded_addr_sk, cr_returning_addr_sk (INTEGER) -> customer_address_opt.ca_address_sk
- cr_call_center_sk (INTEGER) -> call_center_opt.cc_call_center_sk
- cr_catalog_page_sk (INTEGER) -> catalog_page_opt.cp_catalog_page_sk
- cr_ship_mode_sk (INTEGER) -> ship_mode_opt.sm_ship_mode_sk
- cr_warehouse_sk (INTEGER) -> warehouse_opt.w_warehouse_sk
- cr_reason_sk (INTEGER) -> reason_opt.r_reason_sk
- cr_order_number (INTEGER)
- cr_return_quantity (INTEGER)
- cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost (DECIMAL)
- cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss (DECIMAL)

TABLA web_returns_opt:
- wr_returned_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- wr_item_sk (INTEGER) -> item_opt.i_item_sk
- wr_refunded_customer_sk, wr_returning_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- wr_refunded_cdemo_sk, wr_returning_cdemo_sk (INTEGER) -> customer_demographics_opt.cd_demo_sk
- wr_refunded_hdemo_sk, wr_returning_hdemo_sk (INTEGER) -> household_demographics_opt.hd_demo_sk
- wr_refunded_addr_sk, wr_returning_addr_sk (INTEGER) -> customer_address_opt.ca_address_sk
- wr_web_page_sk (INTEGER) -> web_page_opt.wp_web_page_sk
- wr_reason_sk (INTEGER) -> reason_opt.r_reason_sk
- wr_order_number (INTEGER)
- wr_return_quantity (INTEGER)
- wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost (DECIMAL)
- wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss (DECIMAL)

TABLA inventory_opt:
- inv_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- d_year (INTEGER), d_moy (INTEGER) -- columnas de PARTICION
- inv_item_sk (INTEGER) -> item_opt.i_item_sk
- inv_warehouse_sk (INTEGER) -> warehouse_opt.w_warehouse_sk
- inv_quantity_on_hand (INTEGER)

============================================================
TABLAS DE DIMENSION (sin particionar)
============================================================

TABLA customer_opt:
- c_customer_sk (INTEGER, clave primaria), c_customer_id (TEXT)
- c_current_cdemo_sk -> customer_demographics_opt.cd_demo_sk
- c_current_hdemo_sk -> household_demographics_opt.hd_demo_sk
- c_current_addr_sk -> customer_address_opt.ca_address_sk
- c_salutation, c_first_name, c_last_name (TEXT)
- c_preferred_cust_flag (TEXT), c_birth_day, c_birth_month, c_birth_year (INTEGER)
- c_birth_country (TEXT), c_login (TEXT), c_email_address (TEXT)

TABLA customer_address_opt:
- ca_address_sk (INTEGER, clave primaria), ca_address_id (TEXT)
- ca_street_number, ca_street_name, ca_street_type, ca_suite_number (TEXT)
- ca_city, ca_county, ca_state, ca_zip, ca_country (TEXT)
- ca_gmt_offset (DECIMAL), ca_location_type (TEXT)

TABLA customer_demographics_opt:
- cd_demo_sk (INTEGER, clave primaria)
- cd_gender, cd_marital_status, cd_education_status (TEXT)
- cd_purchase_estimate (INTEGER), cd_credit_rating (TEXT)
- cd_dep_count, cd_dep_employed_count, cd_dep_college_count (INTEGER)

TABLA household_demographics_opt:
- hd_demo_sk (INTEGER, clave primaria)
- hd_income_band_sk (INTEGER) -> income_band_opt.ib_income_band_sk
- hd_buy_potential (TEXT), hd_dep_count, hd_vehicle_count (INTEGER)

TABLA income_band_opt:
- ib_income_band_sk (INTEGER, clave primaria)
- ib_lower_bound, ib_upper_bound (INTEGER)

TABLA date_dim_opt:
- d_date_sk (INTEGER, clave primaria), d_date_id (TEXT), d_date (DATE)
- d_month_seq, d_week_seq, d_quarter_seq (INTEGER)
- d_year, d_dow, d_moy, d_dom, d_qoy (INTEGER)
- d_fy_year, d_fy_quarter_seq, d_fy_week_seq (INTEGER)
- d_day_name, d_quarter_name (TEXT)
- d_holiday, d_weekend, d_following_holiday (TEXT) -- 'Y'/'N'
- d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq (INTEGER)
- d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year (TEXT)

TABLA time_dim_opt:
- t_time_sk (INTEGER, clave primaria), t_time_id (TEXT)
- t_time, t_hour, t_minute, t_second (INTEGER)
- t_am_pm, t_shift, t_sub_shift, t_meal_time (TEXT)

TABLA item_opt:
- i_item_sk (INTEGER, clave primaria), i_item_id (TEXT)
- i_rec_start_date, i_rec_end_date (DATE)
- i_item_desc (TEXT), i_current_price, i_wholesale_cost (DECIMAL)
- i_brand_id (INTEGER), i_brand (TEXT)
- i_class_id (INTEGER), i_class (TEXT)
- i_category_id (INTEGER), i_category (TEXT)
- i_manufact_id (INTEGER), i_manufact (TEXT)
- i_size, i_formulation, i_color, i_units, i_container (TEXT)
- i_manager_id (INTEGER), i_product_name (TEXT)

TABLA store_opt:
- s_store_sk (INTEGER, clave primaria), s_store_id (TEXT)
- s_rec_start_date, s_rec_end_date (DATE)
- s_store_name (TEXT), s_number_employees (INTEGER), s_floor_space (INTEGER)
- s_hours, s_manager (TEXT)
- s_market_id (INTEGER), s_geography_class, s_market_desc, s_market_manager (TEXT)
- s_division_id (INTEGER), s_division_name (TEXT)
- s_company_id (INTEGER), s_company_name (TEXT)
- s_street_number, s_street_name, s_street_type, s_suite_number (TEXT)
- s_city, s_county, s_state, s_zip, s_country (TEXT)
- s_gmt_offset, s_tax_precentage (DECIMAL)

TABLA warehouse_opt:
- w_warehouse_sk (INTEGER, clave primaria), w_warehouse_id (TEXT)
- w_warehouse_name (TEXT), w_warehouse_sq_ft (INTEGER)
- w_street_number, w_street_name, w_street_type, w_suite_number (TEXT)
- w_city, w_county, w_state, w_zip, w_country (TEXT), w_gmt_offset (DECIMAL)

TABLA promotion_opt:
- p_promo_sk (INTEGER, clave primaria), p_promo_id (TEXT)
- p_start_date_sk, p_end_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- p_item_sk (INTEGER) -> item_opt.i_item_sk
- p_cost (DECIMAL), p_response_target (INTEGER), p_promo_name (TEXT)
- p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo (TEXT) -- 'Y'/'N'
- p_channel_details (TEXT), p_purpose (TEXT), p_discount_active (TEXT)

TABLA reason_opt:
- r_reason_sk (INTEGER, clave primaria), r_reason_id (TEXT), r_reason_desc (TEXT)

TABLA ship_mode_opt:
- sm_ship_mode_sk (INTEGER, clave primaria), sm_ship_mode_id (TEXT)
- sm_type, sm_code, sm_carrier, sm_contract (TEXT)

TABLA call_center_opt:
- cc_call_center_sk (INTEGER, clave primaria), cc_call_center_id (TEXT)
- cc_rec_start_date, cc_rec_end_date (DATE)
- cc_name, cc_class (TEXT), cc_employees, cc_sq_ft (INTEGER), cc_hours, cc_manager (TEXT)
- cc_mkt_id (INTEGER), cc_mkt_class, cc_mkt_desc, cc_market_manager (TEXT)
- cc_division (INTEGER), cc_division_name (TEXT), cc_company (INTEGER), cc_company_name (TEXT)
- cc_street_number, cc_street_name, cc_street_type, cc_suite_number (TEXT)
- cc_city, cc_county, cc_state, cc_zip, cc_country (TEXT)
- cc_gmt_offset, cc_tax_percentage (DECIMAL)

TABLA catalog_page_opt:
- cp_catalog_page_sk (INTEGER, clave primaria), cp_catalog_page_id (TEXT)
- cp_start_date_sk, cp_end_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- cp_department (TEXT), cp_catalog_number, cp_catalog_page_number (INTEGER)
- cp_description (TEXT), cp_type (TEXT)

TABLA web_page_opt:
- wp_web_page_sk (INTEGER, clave primaria), wp_web_page_id (TEXT)
- wp_rec_start_date, wp_rec_end_date (DATE)
- wp_creation_date_sk, wp_access_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- wp_autogen_flag (TEXT), wp_customer_sk (INTEGER) -> customer_opt.c_customer_sk
- wp_url, wp_type (TEXT)
- wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count (INTEGER)

TABLA web_site_opt:
- web_site_sk (INTEGER, clave primaria), web_site_id (TEXT)
- web_rec_start_date, web_rec_end_date (DATE)
- web_name (TEXT), web_open_date_sk, web_close_date_sk (INTEGER) -> date_dim_opt.d_date_sk
- web_class, web_manager (TEXT), web_mkt_id (INTEGER), web_mkt_class, web_mkt_desc, web_market_manager (TEXT)
- web_company_id (INTEGER), web_company_name (TEXT)
- web_street_number, web_street_name, web_street_type, web_suite_number (TEXT)
- web_city, web_county, web_state, web_zip, web_country (TEXT)
- web_gmt_offset, web_tax_percentage (DECIMAL)

REGLAS:
1. Responde UNICAMENTE con codigo SQL valido para Hive/Spark SQL.
2. No agregues explicaciones, comentarios, ni texto adicional.
3. No uses la palabra "sql" ni comillas triples (```), solo el SQL puro.
4. Usa los nombres de tabla y columna EXACTAMENTE como estan definidos arriba
   (todas las tablas llevan el sufijo "_opt").
5. Si la consulta solo necesita filtrar o agrupar por año y/o mes en una tabla de
   hechos, usa d_year / d_moy directamente desde esa tabla y NO hagas JOIN con
   date_dim_opt.
6. Para comparar canales (tienda/catalogo/web) combina store_sales_opt,
   catalog_sales_opt y web_sales_opt con UNION ALL.
7. Si la pregunta no especifica una cantidad exacta de resultados (ej. "top productos",
   "mejores clientes"), usa LIMIT 10 por defecto.
8. Genera siempre SELECT de solo lectura. Nunca generes DROP, DELETE, UPDATE, INSERT,
   ALTER, TRUNCATE ni CREATE.

Ejemplo de pregunta: "Cuantos clientes hay en total?"
Ejemplo de respuesta: SELECT COUNT(*) FROM customer_opt;

Ejemplo de pregunta: "Cuales son los 5 productos mas caros?"
Ejemplo de respuesta: SELECT i_product_name, i_current_price FROM item_opt ORDER BY i_current_price DESC LIMIT 5;

Ejemplo de pregunta: "Que productos generaron mayores ingresos en tienda?"
Ejemplo de respuesta: SELECT i.i_product_name, SUM(ss.ss_net_paid) AS total_revenue FROM store_sales_opt ss JOIN item_opt i ON ss.ss_item_sk = i.i_item_sk GROUP BY i.i_product_name ORDER BY total_revenue DESC LIMIT 10;

Ejemplo de pregunta: "Cual fue el mes con mayores ingresos?"
Ejemplo de respuesta: SELECT d_year, d_moy, SUM(ss_net_paid) AS total_ingresos FROM store_sales_opt GROUP BY d_year, d_moy ORDER BY total_ingresos DESC LIMIT 1;

Ejemplo de pregunta: "Que canal de venta genero mas ingresos en 2002?"
Ejemplo de respuesta: SELECT canal, SUM(total_ventas) AS total_ingresos FROM ( SELECT 'STORE' AS canal, d_year, ss_net_paid AS total_ventas FROM store_sales_opt UNION ALL SELECT 'CATALOG' AS canal, d_year, cs_net_paid AS total_ventas FROM catalog_sales_opt UNION ALL SELECT 'WEB' AS canal, d_year, ws_net_paid AS total_ventas FROM web_sales_opt ) v WHERE d_year = 2002 GROUP BY canal ORDER BY total_ingresos DESC;
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