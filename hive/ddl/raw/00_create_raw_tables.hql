CREATE DATABASE IF NOT EXISTS retail_tpcds_raw;

USE retail_tpcds_raw;

DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS date_dim;

CREATE EXTERNAL TABLE customer (
    c_customer_sk INT,
    c_customer_id STRING,
    c_current_cdemo_sk INT,
    c_current_hdemo_sk INT,
    c_current_addr_sk INT,
    c_first_shipto_date_sk INT,
    c_first_sales_date_sk INT,
    c_salutation STRING,
    c_first_name STRING,
    c_last_name STRING,
    c_preferred_cust_flag STRING,
    c_birth_day INT,
    c_birth_month INT,
    c_birth_year INT,
    c_birth_country STRING,
    c_login STRING,
    c_email_address STRING,
    c_last_review_date_sk INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/datasets/tpcds/raw/customer';

CREATE EXTERNAL TABLE item (
    i_item_sk INT,
    i_item_id STRING,
    i_rec_start_date DATE,
    i_rec_end_date DATE,
    i_item_desc STRING,
    i_current_price DECIMAL(7,2),
    i_wholesale_cost DECIMAL(7,2),
    i_brand_id INT,
    i_brand STRING,
    i_class_id INT,
    i_class STRING,
    i_category_id INT,
    i_category STRING,
    i_manufact_id INT,
    i_manufact STRING,
    i_size STRING,
    i_formulation STRING,
    i_color STRING,
    i_units STRING,
    i_container STRING,
    i_manager_id INT,
    i_product_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/datasets/tpcds/raw/item';

CREATE EXTERNAL TABLE store (
    s_store_sk INT,
    s_store_id STRING,
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk INT,
    s_store_name STRING,
    s_number_employees INT,
    s_floor_space INT,
    s_hours STRING,
    s_manager STRING,
    s_market_id INT,
    s_geography_class STRING,
    s_market_desc STRING,
    s_market_manager STRING,
    s_division_id INT,
    s_division_name STRING,
    s_company_id INT,
    s_company_name STRING,
    s_street_number STRING,
    s_street_name STRING,
    s_street_type STRING,
    s_suite_number STRING,
    s_city STRING,
    s_county STRING,
    s_state STRING,
    s_zip STRING,
    s_country STRING,
    s_gmt_offset DECIMAL(5,2),
    s_tax_precentage DECIMAL(5,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/datasets/tpcds/raw/store';

CREATE EXTERNAL TABLE date_dim (
    d_date_sk INT,
    d_date_id STRING,
    d_date DATE,
    d_month_seq INT,
    d_week_seq INT,
    d_quarter_seq INT,
    d_year INT,
    d_dow INT,
    d_moy INT,
    d_dom INT,
    d_qoy INT,
    d_fy_year INT,
    d_fy_quarter_seq INT,
    d_fy_week_seq INT,
    d_day_name STRING,
    d_quarter_name STRING,
    d_holiday STRING,
    d_weekend STRING,
    d_following_holiday STRING,
    d_first_dom INT,
    d_last_dom INT,
    d_same_day_ly INT,
    d_same_day_lq INT,
    d_current_day STRING,
    d_current_week STRING,
    d_current_month STRING,
    d_current_quarter STRING,
    d_current_year STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/datasets/tpcds/raw/date_dim';

CREATE EXTERNAL TABLE store_sales (
    ss_sold_date_sk INT,
    ss_sold_time_sk INT,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DECIMAL(7,2),
    ss_list_price DECIMAL(7,2),
    ss_sales_price DECIMAL(7,2),
    ss_ext_discount_amt DECIMAL(7,2),
    ss_ext_sales_price DECIMAL(7,2),
    ss_ext_wholesale_cost DECIMAL(7,2),
    ss_ext_list_price DECIMAL(7,2),
    ss_ext_tax DECIMAL(7,2),
    ss_coupon_amt DECIMAL(7,2),
    ss_net_paid DECIMAL(7,2),
    ss_net_paid_inc_tax DECIMAL(7,2),
    ss_net_profit DECIMAL(7,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/datasets/tpcds/raw/store_sales';