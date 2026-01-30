WITH
aws AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date
        ,billing_origin
        ,aws_product_name
        ,CAST(SUM(unblended_cost + reservation_cost) AS decimal(10,2)) AS cost
    FROM
        {{ ref('aws_costs_marketplace') }}
    GROUP BY
        3,2,1
)

SELECT * FROM aws
