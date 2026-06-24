USE retail_tpcds_opt;

SELECT
    canal,
    d_year,
    d_moy,
    ROUND(SUM(ventas), 2) AS total_ventas,
    ROUND(SUM(devoluciones), 2) AS total_devoluciones,
    ROUND(SUM(ventas) - SUM(devoluciones), 2) AS ingreso_neto
FROM (
    SELECT
        'STORE' AS canal,
        d_year,
        d_moy,
        ss_net_paid AS ventas,
        0 AS devoluciones
    FROM store_sales_opt

    UNION ALL

    SELECT
        'STORE' AS canal,
        d_year,
        d_moy,
        0 AS ventas,
        sr_return_amt AS devoluciones
    FROM store_returns_opt

    UNION ALL

    SELECT
        'CATALOG' AS canal,
        d_year,
        d_moy,
        cs_net_paid AS ventas,
        0 AS devoluciones
    FROM catalog_sales_opt

    UNION ALL

    SELECT
        'CATALOG' AS canal,
        d_year,
        d_moy,
        0 AS ventas,
        cr_return_amount AS devoluciones
    FROM catalog_returns_opt

    UNION ALL

    SELECT
        'WEB' AS canal,
        d_year,
        d_moy,
        ws_net_paid AS ventas,
        0 AS devoluciones
    FROM web_sales_opt

    UNION ALL

    SELECT
        'WEB' AS canal,
        d_year,
        d_moy,
        0 AS ventas,
        wr_return_amt AS devoluciones
    FROM web_returns_opt
) resumen
GROUP BY
    canal,
    d_year,
    d_moy
ORDER BY
    d_year,
    d_moy,
    ingreso_neto DESC;