USE retail_tpcds_opt;

SELECT
    d.d_day_name,
    ROUND(SUM(ss.ss_net_paid), 2) AS total_ventas
FROM store_sales_opt ss
INNER JOIN date_dim_opt d
    ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY
    d.d_day_name
ORDER BY
    total_ventas DESC;