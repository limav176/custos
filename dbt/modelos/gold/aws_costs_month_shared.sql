-- Custo total shared
with shared_total as (
    SELECT
        DATE_TRUNC('month', CAST(start_date as TIMESTAMP)) as start_date
        ,billing_origin
        ,aws_product_name
        ,SUM(unblended_cost + reservation_cost) as cost
    FROM
        {{ ref('aws_costs_shared') }}
    GROUP BY
        3,2,1
),

-- Rate por circulo
rates as (
    SELECT DISTINCT
        start_date
        ,circle_id
        ,SUM(cost) OVER(PARTITION BY start_date,circle_id) as cost
        ,SUM(cost) OVER(PARTITION BY start_date) as total_cost
        ,CAST(SUM(cost) OVER(PARTITION BY start_date,circle_id) AS DECIMAL(38,20))/CAST(SUM(cost) OVER(PARTITION BY start_date) AS DECIMAL(38,20)) as rate
    FROM
        {{ ref('aws_costs_month_usage') }}
)

SELECT
    t.start_date as start_date
    ,t.billing_origin
    ,t.aws_product_name
    ,r.circle_id as circle_id
    ,r.rate * t.cost as cost
FROM
    shared_total t
LEFT JOIN
    rates r
ON
    t.start_date = r.start_date
