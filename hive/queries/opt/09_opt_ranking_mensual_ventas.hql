USE retail_tpcds_opt;

SELECT
    ss.d_year,
    ss.d_moy,
    ROUND(SUM(ss.ss_net_paid), 2) AS total_ventas
FROM store_sales_opt ss
GROUP BY
    ss.d_year,
    ss.d_moy
ORDER BY
    total_ventas DESC;