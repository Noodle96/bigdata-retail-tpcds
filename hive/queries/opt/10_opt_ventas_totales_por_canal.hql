USE retail_tpcds_opt;

SELECT
    canal,
    d_year,
    d_moy,
    ROUND(SUM(total_ventas), 2) AS total_ventas
FROM (
    SELECT
        'STORE' AS canal,
        d_year,
        d_moy,
        ss_net_paid AS total_ventas
    FROM store_sales_opt

    UNION ALL

    SELECT
        'CATALOG' AS canal,
        d_year,
        d_moy,
        cs_net_paid AS total_ventas
    FROM catalog_sales_opt

    UNION ALL

    SELECT
        'WEB' AS canal,
        d_year,
        d_moy,
        ws_net_paid AS total_ventas
    FROM web_sales_opt
) ventas
GROUP BY
    canal,
    d_year,
    d_moy
ORDER BY
    d_year,
    d_moy,
    total_ventas DESC;