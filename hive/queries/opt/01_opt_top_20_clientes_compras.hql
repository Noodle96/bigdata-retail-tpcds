USE retail_tpcds_opt;

SELECT
    c.c_customer_id,
    COUNT(*) AS total_compras
FROM store_sales_opt ss
INNER JOIN customer_opt c
    ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY c.c_customer_id
ORDER BY total_compras DESC
LIMIT 20;