USE retail_tpcds_opt;

SELECT
    ranked.s_store_id,
    ranked.s_store_name,
    ranked.i_item_id,
    ranked.i_product_name,
    ranked.total_cantidad_vendida
FROM (
    SELECT
        s.s_store_id,
        s.s_store_name,
        i.i_item_id,
        i.i_product_name,
        SUM(ss.ss_quantity) AS total_cantidad_vendida,
        ROW_NUMBER() OVER (
            PARTITION BY s.s_store_id
            ORDER BY SUM(ss.ss_quantity) DESC
        ) AS ranking_producto
    FROM store_sales_opt ss
    INNER JOIN store_opt s
        ON ss.ss_store_sk = s.s_store_sk
    INNER JOIN item_opt i
        ON ss.ss_item_sk = i.i_item_sk
    GROUP BY
        s.s_store_id,
        s.s_store_name,
        i.i_item_id,
        i.i_product_name
) ranked
WHERE ranked.ranking_producto <= 5
ORDER BY
    ranked.s_store_id,
    ranked.ranking_producto;