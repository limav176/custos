-- Tabela para backfill de custos de datadog de JAN-24 a ABR-24

WITH months_total AS (
    SELECT
        start_date,
        product_name,
        product_group,
        billing_type,
        sum(cost) as cost
    FROM
        {{ ref('datadog_costs') }}
    WHERE
        (start_date BETWEEN CAST('2023-12-01' AS TIMESTAMP) AND CAST('2024-04-01' AS TIMESTAMP))
    GROUP BY 4,3,2,1
),

-- Custo rateado por círculo baseados no mes de junho de 2024
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
        ,SUM(cost) OVER(PARTITION BY product_name, billing_type) AS total_cost_by_billing_type
        ,SUM(cost) OVER(PARTITION BY service, product_name, billing_type) AS service_cost
        ,CAST(SUM(cost) OVER(PARTITION BY service, product_name, billing_type) AS decimal(38,12)) /
        CAST(SUM(cost) OVER(PARTITION BY product_name, billing_type) AS decimal(38,12)) AS rate
    FROM
        {{ ref('datadog_costs') }}
    WHERE
        month(start_date) = 6
)

SELECT
   mt.start_date
   ,'backfill' as service
   ,cr.circle_id as circle_id
   ,cr.circle_type as circle_type
   ,cr.product_name
   ,cr.product_group
   ,cr.billing_type
   ,cr.invoice_status
   ,ROUND(mt.cost * cr.rate ,2) as cost
FROM
    circle_shared_rates cr
FULL OUTER join
    months_total mt
ON mt.billing_type=cr.billing_type AND mt.product_name=cr.product_name
