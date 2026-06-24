import os
import re
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from google import genai
from pyhive import hive
from pyspark.sql import SparkSession


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("No se encontró GEMINI_API_KEY en el archivo .env")

client = genai.Client(api_key=GEMINI_API_KEY)

DATABASE_NAME = "retail_tpcds_opt"
HIVE_HOST = "localhost"
HIVE_PORT = 10000
HIVE_USER = "hadoop"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORIAL_PATH = os.path.join(BASE_DIR, "historial.json")


# ============================================================
# TABLAS DEL DATA WAREHOUSE OPTIMIZADO
# ============================================================

FACT_TABLES = [
    "store_sales_opt",
    "catalog_sales_opt",
    "web_sales_opt",
    "store_returns_opt",
    "catalog_returns_opt",
    "web_returns_opt",
    "inventory_opt",
]

DIMENSION_TABLES = [
    "customer_opt",
    "customer_address_opt",
    "customer_demographics_opt",
    "household_demographics_opt",
    "income_band_opt",
    "date_dim_opt",
    "time_dim_opt",
    "item_opt",
    "store_opt",
    "warehouse_opt",
    "promotion_opt",
    "reason_opt",
    "ship_mode_opt",
    "call_center_opt",
    "catalog_page_opt",
    "web_page_opt",
    "web_site_opt",
]

TABLAS_CONOCIDAS = FACT_TABLES + DIMENSION_TABLES


# ============================================================
# CONTEXTO PARA EL LLM
# ============================================================

ESQUEMA = """
Eres un experto en SQL para Apache Hive y Spark SQL.
Trabajas sobre la base de datos retail_tpcds_opt, que contiene tablas optimizadas en formato Parquet.

IMPORTANTE:
- Todas las tablas usan sufijo _opt.
- Las tablas de hechos están optimizadas y particionadas por d_year y d_moy.
- Genera únicamente SQL válido para Hive/Spark SQL.
- No uses SQLite.
- No uses comillas triples.
- No agregues explicaciones.
- No agregues comentarios.
- No uses la palabra sql.
- Si la pregunta no especifica cantidad, usa LIMIT 10.
- Para ingresos o ventas usa net_paid cuando exista.
- Para devoluciones usa return_amt, return_amount o net_loss según corresponda.
- Usa aliases claros para las tablas.

BASE DE DATOS:
retail_tpcds_opt

TABLAS DE HECHOS:

store_sales_opt:
ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk,
ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number,
ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price,
ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost,
ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid,
ss_net_paid_inc_tax, ss_net_profit, d_year, d_moy.

catalog_sales_opt:
cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk,
cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk,
cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk,
cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk,
cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost,
cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price,
cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt,
cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship,
cs_net_paid_inc_ship_tax, cs_net_profit, d_year, d_moy.

web_sales_opt:
ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk,
ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk,
ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk,
ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk,
ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost,
ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price,
ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt,
ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship,
ws_net_paid_inc_ship_tax, ws_net_profit, d_year, d_moy.

store_returns_opt:
sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk,
sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk,
sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax,
sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash,
sr_reversed_charge, sr_store_credit, sr_net_loss, d_year, d_moy.

catalog_returns_opt:
cr_returned_date_sk, cr_returned_time_sk, cr_item_sk,
cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk,
cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk,
cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk,
cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk,
cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax,
cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash,
cr_reversed_charge, cr_store_credit, cr_net_loss, d_year, d_moy.

web_returns_opt:
wr_returned_date_sk, wr_returned_time_sk, wr_item_sk,
wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk,
wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk,
wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk,
wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt,
wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost,
wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss,
d_year, d_moy.

inventory_opt:
inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand,
d_year, d_moy.

TABLAS DE DIMENSIÓN:

customer_opt:
c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk,
c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk,
c_salutation, c_first_name, c_last_name, c_preferred_cust_flag,
c_birth_day, c_birth_month, c_birth_year, c_birth_country,
c_login, c_email_address, c_last_review_date_sk.

customer_address_opt:
ca_address_sk, ca_address_id, ca_street_number, ca_street_name,
ca_street_type, ca_suite_number, ca_city, ca_county, ca_state,
ca_zip, ca_country, ca_gmt_offset, ca_location_type.

customer_demographics_opt:
cd_demo_sk, cd_gender, cd_marital_status, cd_education_status,
cd_purchase_estimate, cd_credit_rating, cd_dep_count,
cd_dep_employed_count, cd_dep_college_count.

household_demographics_opt:
hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count,
hd_vehicle_count.

income_band_opt:
ib_income_band_sk, ib_lower_bound, ib_upper_bound.

date_dim_opt:
d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq,
d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year,
d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name,
d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom,
d_same_day_ly, d_same_day_lq, d_current_day, d_current_week,
d_current_month, d_current_quarter, d_current_year.

time_dim_opt:
t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second,
t_am_pm, t_shift, t_sub_shift, t_meal_time.

item_opt:
i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date,
i_item_desc, i_current_price, i_wholesale_cost, i_brand_id,
i_brand, i_class_id, i_class, i_category_id, i_category,
i_manufact_id, i_manufact, i_size, i_formulation, i_color,
i_units, i_container, i_manager_id, i_product_name.

store_opt:
s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date,
s_closed_date_sk, s_store_name, s_number_employees, s_floor_space,
s_hours, s_manager, s_market_id, s_geography_class, s_market_desc,
s_market_manager, s_division_id, s_division_name, s_company_id,
s_company_name, s_street_number, s_street_name, s_street_type,
s_suite_number, s_city, s_county, s_state, s_zip, s_country,
s_gmt_offset, s_tax_precentage.

warehouse_opt:
w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft,
w_street_number, w_street_name, w_street_type, w_suite_number,
w_city, w_county, w_state, w_zip, w_country, w_gmt_offset.

promotion_opt:
p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk,
p_item_sk, p_cost, p_response_target, p_promo_name,
p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv,
p_channel_radio, p_channel_press, p_channel_event, p_channel_demo,
p_channel_details, p_purpose, p_discount_active.

reason_opt:
r_reason_sk, r_reason_id, r_reason_desc.

ship_mode_opt:
sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code,
sm_carrier, sm_contract.

call_center_opt:
cc_call_center_sk, cc_call_center_id, cc_rec_start_date,
cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name,
cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager,
cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager,
cc_division, cc_division_name, cc_company, cc_company_name,
cc_street_number, cc_street_name, cc_street_type, cc_suite_number,
cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset,
cc_tax_percentage.

catalog_page_opt:
cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk,
cp_end_date_sk, cp_department, cp_catalog_number,
cp_catalog_page_number, cp_description, cp_type.

web_page_opt:
wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date,
wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag,
wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count,
wp_image_count, wp_max_ad_count.

web_site_opt:
web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date,
web_name, web_open_date_sk, web_close_date_sk, web_class,
web_manager, web_mkt_id, web_mkt_class, web_mkt_desc,
web_market_manager, web_company_id, web_company_name,
web_street_number, web_street_name, web_street_type,
web_suite_number, web_city, web_county, web_state, web_zip,
web_country, web_gmt_offset, web_tax_percentage.

RELACIONES PRINCIPALES:
store_sales_opt.ss_customer_sk = customer_opt.c_customer_sk
store_sales_opt.ss_item_sk = item_opt.i_item_sk
store_sales_opt.ss_store_sk = store_opt.s_store_sk
store_sales_opt.ss_sold_date_sk = date_dim_opt.d_date_sk
store_sales_opt.ss_promo_sk = promotion_opt.p_promo_sk

catalog_sales_opt.cs_bill_customer_sk = customer_opt.c_customer_sk
catalog_sales_opt.cs_item_sk = item_opt.i_item_sk
catalog_sales_opt.cs_sold_date_sk = date_dim_opt.d_date_sk
catalog_sales_opt.cs_call_center_sk = call_center_opt.cc_call_center_sk
catalog_sales_opt.cs_catalog_page_sk = catalog_page_opt.cp_catalog_page_sk
catalog_sales_opt.cs_ship_mode_sk = ship_mode_opt.sm_ship_mode_sk
catalog_sales_opt.cs_warehouse_sk = warehouse_opt.w_warehouse_sk

web_sales_opt.ws_bill_customer_sk = customer_opt.c_customer_sk
web_sales_opt.ws_item_sk = item_opt.i_item_sk
web_sales_opt.ws_sold_date_sk = date_dim_opt.d_date_sk
web_sales_opt.ws_web_page_sk = web_page_opt.wp_web_page_sk
web_sales_opt.ws_web_site_sk = web_site_opt.web_site_sk
web_sales_opt.ws_ship_mode_sk = ship_mode_opt.sm_ship_mode_sk
web_sales_opt.ws_warehouse_sk = warehouse_opt.w_warehouse_sk

store_returns_opt.sr_customer_sk = customer_opt.c_customer_sk
store_returns_opt.sr_item_sk = item_opt.i_item_sk
store_returns_opt.sr_store_sk = store_opt.s_store_sk
store_returns_opt.sr_reason_sk = reason_opt.r_reason_sk

catalog_returns_opt.cr_refunded_customer_sk = customer_opt.c_customer_sk
catalog_returns_opt.cr_item_sk = item_opt.i_item_sk
catalog_returns_opt.cr_reason_sk = reason_opt.r_reason_sk

web_returns_opt.wr_refunded_customer_sk = customer_opt.c_customer_sk
web_returns_opt.wr_item_sk = item_opt.i_item_sk
web_returns_opt.wr_reason_sk = reason_opt.r_reason_sk
web_returns_opt.wr_web_page_sk = web_page_opt.wp_web_page_sk

inventory_opt.inv_item_sk = item_opt.i_item_sk
inventory_opt.inv_warehouse_sk = warehouse_opt.w_warehouse_sk
inventory_opt.inv_date_sk = date_dim_opt.d_date_sk

EJEMPLOS:
Pregunta: Top 10 productos con mayor ingreso.
Respuesta:
SELECT i.i_product_name, ROUND(SUM(ss.ss_net_paid), 2) AS total_ingreso
FROM store_sales_opt ss
JOIN item_opt i ON ss.ss_item_sk = i.i_item_sk
GROUP BY i.i_product_name
ORDER BY total_ingreso DESC
LIMIT 10;

Pregunta: Ventas por tienda.
Respuesta:
SELECT s.s_store_id, s.s_store_name, ROUND(SUM(ss.ss_net_paid), 2) AS total_ventas
FROM store_sales_opt ss
JOIN store_opt s ON ss.ss_store_sk = s.s_store_sk
GROUP BY s.s_store_id, s.s_store_name
ORDER BY total_ventas DESC
LIMIT 10;

Pregunta: Ventas totales por canal.
Respuesta:
SELECT canal, ROUND(SUM(total_ventas), 2) AS total_ventas
FROM (
    SELECT 'STORE' AS canal, ss_net_paid AS total_ventas FROM store_sales_opt
    UNION ALL
    SELECT 'CATALOG' AS canal, cs_net_paid AS total_ventas FROM catalog_sales_opt
    UNION ALL
    SELECT 'WEB' AS canal, ws_net_paid AS total_ventas FROM web_sales_opt
) t
GROUP BY canal
ORDER BY total_ventas DESC;
"""


# ============================================================
# UTILIDADES
# ============================================================

def limpiar_sql(sql: str) -> str:
    sql = sql.strip()
    sql = sql.replace("```sql", "")
    sql = sql.replace("```", "")
    sql = sql.strip()
    sql = sql.rstrip(";")
    return sql


def validar_sql_seguro(sql: str) -> None:
    sql_lower = sql.lower().strip()

    comandos_prohibidos = [
        "drop ",
        "delete ",
        "truncate ",
        "insert ",
        "update ",
        "alter ",
        "create ",
        "msck ",
        "repair ",
        "load ",
        "set ",
    ]

    if not sql_lower.startswith("select"):
        raise ValueError("Solo se permiten consultas SELECT.")

    for comando in comandos_prohibidos:
        if comando in sql_lower:
            raise ValueError(f"Consulta no permitida. Contiene: {comando.strip()}.")


def detectar_tablas(sql: str) -> List[str]:
    sql_lower = sql.lower()
    tablas = []

    for tabla in TABLAS_CONOCIDAS:
        patron = r"\b" + re.escape(tabla.lower()) + r"\b"
        if re.search(patron, sql_lower):
            tablas.append(tabla)

    return tablas


def guardar_en_historial(registro: Dict[str, Any]) -> None:
    historial: List[Dict[str, Any]] = []

    if os.path.exists(HISTORIAL_PATH):
        try:
            with open(HISTORIAL_PATH, "r", encoding="utf-8") as f:
                historial = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            historial = []

    historial.append(registro)
    historial = historial[-50:]

    with open(HISTORIAL_PATH, "w", encoding="utf-8") as f:
        json.dump(historial, f, ensure_ascii=False, indent=2)


# ============================================================
# SKILL 1: IDENTIFICACIÓN / GENERACIÓN SQL
# ============================================================

def generar_sql(pregunta: str) -> str:
    prompt = f"""
{ESQUEMA}

Pregunta del usuario:
{pregunta}

SQL:
"""

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    sql = limpiar_sql(response.text)
    validar_sql_seguro(sql)

    return sql


# ============================================================
# SKILL 2: SELECCIÓN DE MOTOR
# ============================================================

def elegir_motor(sql: str) -> str:
    sql_lower = sql.lower()

    cantidad_joins = sql_lower.count(" join ")
    cantidad_union = sql_lower.count(" union ")
    cantidad_select = sql_lower.count("select")

    tiene_group_by = "group by" in sql_lower
    tiene_order_by = "order by" in sql_lower
    tiene_window = "over (" in sql_lower
    tiene_subquery = cantidad_select > 1

    if cantidad_union >= 1:
        return "spark"

    if cantidad_joins >= 2:
        return "spark"

    if tiene_window:
        return "spark"

    if tiene_subquery:
        return "spark"

    if tiene_group_by and tiene_order_by:
        return "spark"

    return "hive"


# ============================================================
# SKILL 3: EJECUCIÓN EN HIVE
# ============================================================

def ejecutar_hive(sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    conn = hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        username=HIVE_USER,
        database=DATABASE_NAME,
        auth="NOSASL",
    )

    cursor = conn.cursor()
    cursor.execute(sql)

    columnas = [desc[0] for desc in cursor.description]
    resultados = cursor.fetchall()

    cursor.close()
    conn.close()

    return columnas, resultados


# ============================================================
# SKILL 4: EJECUCIÓN EN SPARK
# ============================================================

def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("AgenteSQLRetailTPCDS")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"USE {DATABASE_NAME}")

    return spark


def ejecutar_spark(sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    spark = get_spark_session()

    df = spark.sql(sql)

    columnas = df.columns
    filas = df.collect()
    resultados = [tuple(row) for row in filas]

    return columnas, resultados


def ejecutar_sql(motor: str, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    if motor == "spark":
        return ejecutar_spark(sql)

    return ejecutar_hive(sql)


# ============================================================
# SKILL 5: RESPUESTA FINAL
# ============================================================

def generar_respuesta_usuario(
    pregunta: str,
    motor: str,
    sql: str,
    columnas: List[str],
    resultados: List[Tuple[Any, ...]],
) -> str:
    muestra_resultados = resultados[:20]

    prompt = f"""
El usuario preguntó:
{pregunta}

Se ejecutó una consulta en el motor {motor.upper()}.

Columnas:
{columnas}

Resultados:
{muestra_resultados}

Responde en español de forma breve, clara y orientada al negocio.
No menciones SQL, motores, tablas internas ni detalles técnicos.
"""

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    return response.text.strip()


# ============================================================
# FLUJO PRINCIPAL DEL AGENTE
# ============================================================

def preguntar(pregunta: str) -> None:
    print(f"\nPregunta: {pregunta}")

    inicio_total = time.time()

    sql = ""
    motor = "hive"
    tablas: List[str] = []

    try:
        sql = generar_sql(pregunta)
        motor = elegir_motor(sql)
        tablas = detectar_tablas(sql)

        print("\nSQL generado:")
        print(sql)
        print(f"\nMotor seleccionado: {motor.upper()}")
        print(f"Tablas detectadas: {tablas}")

        inicio_ejecucion = time.time()
        columnas, resultados = ejecutar_sql(motor, sql)
        duracion = round(time.time() - inicio_ejecucion, 3)

        print(f"\nColumnas: {columnas}")
        print("Resultados:")

        for fila in resultados[:20]:
            print(fila)

        if len(resultados) > 20:
            print(f"... mostrando 20 de {len(resultados)} filas")

        guardar_en_historial({
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "pregunta": pregunta,
            "sql": sql,
            "motor": motor,
            "tablas": tablas,
            "duracion_seg": duracion,
            "filas_devueltas": len(resultados),
            "estado": "ok",
        })

        respuesta = generar_respuesta_usuario(
            pregunta=pregunta,
            motor=motor,
            sql=sql,
            columnas=columnas,
            resultados=resultados,
        )

        print(f"\nRespuesta:")
        print(respuesta)

    except Exception as e:
        duracion = round(time.time() - inicio_total, 3)

        print(f"\nError al ejecutar: {e}")

        guardar_en_historial({
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "pregunta": pregunta,
            "sql": sql,
            "motor": motor,
            "tablas": tablas,
            "duracion_seg": duracion,
            "filas_devueltas": 0,
            "estado": "error",
            "error": str(e),
        })


# ============================================================
# CLI
# ============================================================

def main() -> None:
    print("=" * 70)
    print("AGENTE DE ANÁLISIS - DATA WAREHOUSE RETAIL TPC-DS")
    print("=" * 70)
    print("Base de datos:", DATABASE_NAME)
    print("Tablas disponibles:", len(TABLAS_CONOCIDAS))
    print("Dashboard: dashboard.html")
    print("Historial:", HISTORIAL_PATH)
    print("Escribe tu pregunta en lenguaje natural o 'salir' para terminar.")
    print("=" * 70)

    while True:
        pregunta = input("\nTu pregunta: ").strip()

        if pregunta.lower() in ["salir", "exit", "quit"]:
            print("Hasta luego.")
            break

        if not pregunta:
            continue

        preguntar(pregunta)


if __name__ == "__main__":
    main()