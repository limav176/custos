-- Custo total suporte
with support_total as (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) as start_date
        ,DATE_TRUNC('month', CAST(end_date AS TIMESTAMP)) as end_date
        ,sku
        ,CAST(SUM(total_price_cents) AS decimal(10,2))/100 AS cost
    FROM
    	{{ ref('mongodb_atlas_costs_support') }}
    GROUP BY 1,2,3
),

-- Rate por circulo
rates as (
    SELECT DISTINCT
        start_date
        ,end_date
        ,circle_id
        ,SUM(cost) OVER(PARTITION BY start_date,end_date,circle_id) as cost
        ,SUM(cost) OVER(PARTITION BY start_date,end_date) as total_cost
        ,CAST(SUM(cost) OVER(PARTITION BY start_date,end_date,circle_id) AS DECIMAL(38,20))/CAST(SUM(cost) OVER(PARTITION BY start_date,end_date) AS DECIMAL(38,20)) as rate
    FROM
        {{ ref('mongodb_atlas_costs_month_usage') }}
)

SELECT
    t.start_date as start_date
    ,t.end_date as end_date
    ,t.sku
    ,r.circle_id as circle_id
    ,r.rate * t.cost as cost
FROM
    support_total t
LEFT JOIN
    rates r
ON
    t.start_date = r.start_date
    AND t.end_date = r.end_date
