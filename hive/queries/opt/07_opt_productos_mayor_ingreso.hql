USE retail_tpcds_opt;

SELECT
    i.i_item_id,
    i.i_product_name,
    ROUND(SUM(ss.ss_net_paid), 2) AS ingreso_total
FROM store_sales_opt ss
INNER JOIN item_opt i
    ON ss.ss_item_sk = i.i_item_sk
GROUP BY
    i.i_item_id,
    i.i_product_name
ORDER BY
    ingreso_total DESC
LIMIT 20;