WITH bronze_checksum AS (
    SELECT
        start_date
        ,table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM
        {{ ref('stg_checksum_datadog') }}
),

silver_tables AS (
    SELECT
        start_date
        ,'datadog_costs' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('datadog_costs') }}
    UNION ALL
    SELECT
        start_date
        ,'datadog_costs_shared' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('datadog_costs_shared') }}
    UNION ALL
    SELECT
        start_date
        ,'datadog_costs_usage' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('datadog_costs_usage') }}
    UNION ALL
    SELECT
        start_date
        ,'aux_datadog_costs_backfill' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('aux_datadog_costs_backfill') }}
    UNION ALL
    SELECT
        start_date
        ,'aux_datadog_costs_forecast' as table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,cost
    FROM {{ ref('aux_datadog_costs_forecast') }}
),

silver AS (
    SELECT
        start_date
        ,table_name
        ,circle_id
        ,product_name
        ,billing_type
        ,SUM(cost) as cost
    FROM
        silver_tables
    GROUP BY 5,4,3,2,1
)

SELECT * FROM bronze_checksum
UNION ALL
SELECT * FROM silver
