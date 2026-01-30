-- Tabela para backfill de custos de datadog de JAN-24 a ABR-24

WITH months_total AS (
    SELECT
        start_date
        ,product_name
        ,billing_type
        ,sum(cost) as cost
    FROM
       {{ ref('datadog_costs') }}
    WHERE
	invoice_status = 'estimated'
    GROUP BY 3,2,1
),

-- Custo rateado por círculo baseados:
--   - 1 mes antes se ja tivermos os dados completos na datadog para o mes anterior
--   - 2 meses antes se nao tivermos os dados completos na datadog para o mes anterior
circle_shared_rates as (
    SELECT DISTINCT
        circle_id
        ,start_date
        ,service
        ,product_name
        ,product_group
        ,billing_type
        ,circle_type
        ,invoice_status
        ,SUM(cost) OVER(PARTITION BY product_name, billing_type) AS total_cost_by_product_name
        ,SUM(cost) OVER(PARTITION BY service, product_name, billing_type) AS service_cost
        ,CAST(SUM(cost) OVER(PARTITION BY service, product_name, billing_type) AS decimal(38,12)) /
        CAST(SUM(cost) OVER(PARTITION BY product_name, billing_type) AS decimal(38,12)) AS rate
    FROM
        {{ ref('datadog_costs') }}
    WHERE
        month(start_date) = 12 AND year(start_date) = 2024
       -- month(start_date) = (
        --    SELECT extract(MONTH FROM MAX(start_date))
        --    FROM {{ ref('datadog_costs') }}
        --    WHERE invoice_status = 'closed'
       -- )
       -- AND year(start_date) = (
        --    SELECT extract(YEAR FROM MAX(start_date))
       --     FROM {{ ref('datadog_costs') }}
        --    WHERE invoice_status = 'closed'
       -- )
)

SELECT
    mt.start_date
    ,'estimated' as service
    ,cr.circle_id
    ,cr.circle_type
    ,cr.product_name
    ,cr.product_group
    ,cr.billing_type
    ,cr.invoice_status
    ,mt.cost * cr.rate AS cost
FROM
        circle_shared_rates cr
FULL OUTER JOIN
        months_total mt
ON mt.billing_type=cr.billing_type AND mt.product_name=cr.product_name
