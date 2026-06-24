USE retail_tpcds_opt;

SELECT
    s.s_store_id,
    s.s_store_name,
    ROUND(SUM(ss.ss_net_paid), 2) AS total_ventas
FROM store_sales_opt ss
INNER JOIN store_opt s
    ON ss.ss_store_sk = s.s_store_sk
GROUP BY
    s.s_store_id,
    s.s_store_name
ORDER BY total_ventas DESC;