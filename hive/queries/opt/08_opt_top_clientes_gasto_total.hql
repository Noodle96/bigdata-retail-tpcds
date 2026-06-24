USE retail_tpcds_opt;

SELECT
    c.c_customer_id,
    ROUND(SUM(ss.ss_net_paid), 2) AS gasto_total
FROM store_sales_opt ss
INNER JOIN customer_opt c
    ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY
    c.c_customer_id
ORDER BY
    gasto_total DESC
LIMIT 20;