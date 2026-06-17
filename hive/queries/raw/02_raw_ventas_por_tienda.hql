USE retail_tpcds_raw;

SELECT
    s.s_store_id,
    s.s_store_name,
    ROUND(SUM(ss.ss_net_paid), 2) AS total_ventas
FROM store_sales ss
INNER JOIN store s
    ON ss.ss_store_sk = s.s_store_sk
GROUP BY
    s.s_store_id,
    s.s_store_name
ORDER BY total_ventas DESC;