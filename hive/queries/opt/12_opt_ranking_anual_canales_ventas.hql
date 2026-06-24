USE retail_tpcds_opt;

SELECT
    ranked.d_year,
    ranked.canal,
    ranked.total_ventas,
    ranked.ranking_anual
FROM (
    SELECT
        d_year,
        canal,
        ROUND(SUM(total_ventas), 2) AS total_ventas,
        ROW_NUMBER() OVER (
            PARTITION BY d_year
            ORDER BY SUM(total_ventas) DESC
        ) AS ranking_anual
    FROM (
        SELECT
            d_year,
            'STORE' AS canal,
            ss_net_paid AS total_ventas
        FROM store_sales_opt

        UNION ALL

        SELECT
            d_year,
            'CATALOG' AS canal,
            cs_net_paid AS total_ventas
        FROM catalog_sales_opt

        UNION ALL

        SELECT
            d_year,
            'WEB' AS canal,
            ws_net_paid AS total_ventas
        FROM web_sales_opt
    ) ventas
    GROUP BY
        d_year,
        canal
) ranked
ORDER BY
    ranked.d_year,
    ranked.ranking_anual;