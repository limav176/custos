WITH silver_checksum AS (
    SELECT
        start_date
        ,table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM
        {{ ref('silver_checksum_datadog') }}
),

gold_tables AS (
    SELECT
        start_date
        ,'datadog_costs_month_shared' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('datadog_costs_month_shared') }}
    UNION ALL
    SELECT
        start_date
        ,'datadog_costs_month_usage' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('datadog_costs_month_usage') }}
),

gold AS (
    SELECT
        start_date
        ,table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,SUM(cost) as cost
    FROM
        gold_tables
    GROUP BY 5,4,3,2,1
),

fif AS (
    SELECT
        month AS start_date
        ,'circle_month' AS table_name
        ,circle_id
        ,'' as product_name
        ,'' as billing_type
        ,SUM(cost) as cost
    FROM
        {{ ref('circle_month') }}
    WHERE
        origin = 'Datadog'
    GROUP BY 5,4,3,2,1
)

SELECT * FROM silver_checksum
UNION ALL
SELECT * FROM gold
UNION ALL
SELECT * FROM fif
