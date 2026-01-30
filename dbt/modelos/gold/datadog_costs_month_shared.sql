-- Custo total shared
with month_shared_total AS (
    SELECT
        start_date
	    ,product_name
        ,billing_type
        ,CAST(SUM(cost) AS decimal(10,2)) AS cost
    FROM
        {{ ref('datadog_costs_shared') }}
    GROUP BY
        3,2,1
),

-- Custo por círculo sem shared
month_usage_circle as (
    SELECT
        start_date
        ,service
        ,circle_id
        ,circle_type
        ,product_name
        ,product_group
        ,billing_type
        ,invoice_status
        ,SUM(cost) as cost
    FROM
        {{ ref('datadog_costs_month_usage') }}
    GROUP BY 8,7,6,5,4,3,2,1
),

month_usage_total as (
    SELECT
        start_date
        ,billing_type
        ,SUM(cost) as cost
    FROM
        {{ ref('datadog_costs_month_usage') }}
    GROUP BY 2,1
),

circle_total as (
    SELECT
        muc.start_date
        ,muc.billing_type
        ,SUM(CAST(muc.cost AS decimal(38,12))) as total_circle_cost
    FROM
        month_usage_circle muc
    GROUP BY
        muc.start_date, muc.billing_type
),
-- Custo de shared rateado por círculo
circle_shared_rates as (
    SELECT
        muc.start_date as start_date
        ,'shared' as service
        ,muc.circle_id
        ,muc.circle_type
        ,mst.product_name as original_product_name
        ,'shared' as product_name
        ,'shared' as product_group
        ,muc.billing_type
        ,muc.invoice_status
        ,CAST(muc.cost AS decimal(38,12)) / c.total_circle_cost as rate -- % of the circle cost relative to total
        ,mst.cost as total_shared_cost
        ,(CAST(muc.cost AS decimal(38,12)) / c.total_circle_cost) * mst.cost as circle_shared_cost -- Recalculated shared cost
    FROM
        month_usage_circle muc
    LEFT JOIN
        month_shared_total mst
    ON
        muc.start_date = mst.start_date AND muc.billing_type = mst.billing_type
    LEFT JOIN
        circle_total c
    ON
        muc.start_date = c.start_date AND muc.billing_type = c.billing_type
)

SELECT
   start_date
   ,'Datadog' AS origin
   ,service
   ,circle_id
   ,circle_type
   ,original_product_name
   ,product_name
   ,product_group
   ,billing_type
   ,invoice_status
   ,circle_shared_cost as cost
FROM
    circle_shared_rates
