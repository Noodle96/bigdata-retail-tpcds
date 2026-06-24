USE retail_tpcds_opt;

WITH ticket_totals AS (
    SELECT
        ss_customer_sk,
        ss_ticket_number,
        SUM(ss_net_paid) AS total_ticket
    FROM store_sales_opt
    WHERE ss_customer_sk IS NOT NULL
    GROUP BY
        ss_customer_sk,
        ss_ticket_number
)

SELECT
    c.c_customer_id,
    ROUND(AVG(tt.total_ticket), 2) AS ticket_promedio,
    COUNT(*) AS cantidad_tickets
FROM ticket_totals tt
INNER JOIN customer_opt c
    ON tt.ss_customer_sk = c.c_customer_sk
GROUP BY
    c.c_customer_id
ORDER BY
    ticket_promedio DESC;