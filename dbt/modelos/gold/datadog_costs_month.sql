WITH
month AS (
    SELECT
        start_date
        ,service
        ,circle_id
        ,circle_type
        ,original_product_name
        ,product_name
        ,product_group
        ,billing_type
        ,invoice_status
        ,SUM(cost) AS cost
    FROM
        {{ ref('datadog_costs_month_usage') }}
    GROUP BY
        9,8,7,6,5,4,3,2,1

    UNION ALL

    SELECT
        start_date
        ,service
        ,circle_id
        ,circle_type
        ,original_product_name
        ,product_name
        ,product_group
        ,billing_type
        ,invoice_status
        ,SUM(cost) AS cost
    FROM
        {{ ref('datadog_costs_month_shared') }}
    GROUP BY
        9,8,7,6,5,4,3,2,1
),

current_month_agg AS (
    SELECT
        start_date
        ,service
        ,circle_id
        ,circle_type
        ,original_product_name
        ,product_name
        ,product_group
        ,billing_type
        ,invoice_status
        ,SUM(cost) AS cost
    FROM month
    GROUP BY
        9,8,7,6,5,4,3,2,1
),

prev_month_agg AS (
    SELECT
        start_date
        ,service
        ,circle_id
        ,circle_type
        ,original_product_name
        ,product_name
        ,product_group
        ,billing_type
        ,invoice_status
        ,SUM(cost) AS cost
    FROM month
    GROUP BY
        9,8,7,6,5,4,3,2,1
),

source as (
SELECT
     COALESCE(current_month.service, prev_month.service) as service
    ,COALESCE(current_month.circle_id, prev_month.circle_id, '999999999999999999999999') as circle_id
    ,COALESCE(current_month.circle_type, prev_month.circle_type) as circle_type
    ,COALESCE(current_month.original_product_name, prev_month.original_product_name) as original_product_name
    ,COALESCE(current_month.product_name, prev_month.product_name) as product_name
    ,COALESCE(current_month.product_group, prev_month.product_group) as product_group
    ,COALESCE(current_month.billing_type, prev_month.billing_type) as billing_type
    ,COALESCE(current_month.invoice_status, prev_month.invoice_status) as invoice_status
    ,COALESCE(current_month.start_date, prev_month.start_date + interval '1' month) as month
    ,COALESCE(current_month.cost, 0)  as cost
    ,COALESCE(prev_month.start_date, current_month.start_date - interval '1' month)  as previous_month
    ,COALESCE(prev_month.cost, 0) as previous_cost
FROM
    current_month_agg as current_month
FULL OUTER JOIN
    prev_month_agg as prev_month
ON
    current_month.service = prev_month.service
    AND current_month.circle_id = prev_month.circle_id
    AND current_month.circle_type = prev_month.circle_type
    AND current_month.original_product_name = prev_month.original_product_name
    AND current_month.product_name = prev_month.product_name
    AND current_month.product_group = prev_month.product_group
    AND current_month.billing_type = prev_month.billing_type
    AND current_month.invoice_status = prev_month.invoice_status
    AND current_month.start_date - interval '1' month = prev_month.start_date
ORDER BY
    9,8,7,6,5,4,3,2,1 DESC
)

SELECT * FROM source
