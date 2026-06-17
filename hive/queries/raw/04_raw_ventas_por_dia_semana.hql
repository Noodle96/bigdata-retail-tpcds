USE retail_tpcds_raw;

SELECT
    d.d_day_name,
    ROUND(SUM(ss.ss_net_paid), 2) AS total_ventas
FROM store_sales ss
INNER JOIN date_dim d
    ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY
    d.d_day_name
ORDER BY
    total_ventas DESC;