WITH bronze_checksum AS(
    SELECT
        start_date
        ,table_name
        ,circle_id
        ,aws_product_name
        ,cost
    FROM
        {{ ref('stg_checksum_aws') }}
),

staging_checksum AS (
    SELECT
        DATE_TRUNC('month', start_date) AS start_date,
        'stg_aws_cost_usage_report' as table_name,
        circle_id,
        aws_product_name,
        SUM(unblended_cost + reservation_cost) AS cost
    FROM
        {{ ref('stg_aws_cost_usage_report') }}
    GROUP BY 1,2,3,4
),

silver_tables AS (
    SELECT
        DATE_TRUNC('month', start_date) AS start_date,
        'intermediate_aws_cost_usage_report' as table_name,
        circle_id,
        aws_product_name,
        SUM(unblended_cost + reservation_cost) AS cost
    FROM
        {{ ref('intermediate_aws_cost_usage_report') }}
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT
        DATE_TRUNC('month', start_date) AS start_date,
        'silver_aws_cost_usage_report' as table_name,
        circle_id,
        aws_product_name,
        SUM(unblended_cost + reservation_cost) AS cost
    FROM
        {{ ref('silver_aws_cost_usage_report') }}
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT start_date
           ,'aws_costs_marketplace' as table_name
           ,circle_id
           ,aws_product_name
           ,unblended_cost + reservation_cost as cost
    FROM {{ ref('aws_costs_marketplace') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_shared' as table_name
           ,circle_id
           ,aws_product_name
           ,unblended_cost + reservation_cost as cost
    FROM {{ ref('aws_costs_shared') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_support' as table_name
           ,circle_id
           ,aws_product_name
           ,unblended_cost + reservation_cost as cost
    FROM {{ ref('aws_costs_support') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_usage' as table_name
           ,circle_id
           ,aws_product_name
           ,unblended_cost + reservation_cost as cost
    FROM {{ ref('aws_costs_usage') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_untagged' as table_name
           ,circle_id
           ,aws_product_name
           ,unblended_cost + reservation_cost as cost
    FROM {{ ref('aws_costs_untagged') }}
),

silver AS (
    SELECT
        DATE_TRUNC('month', start_date) AS start_date
        ,table_name
        ,circle_id
        ,aws_product_name
        ,SUM(cost)
    FROM
        silver_tables
    GROUP BY 1,2,3,4
)

SELECT * FROM bronze_checksum
UNION ALL
SELECT * FROM staging_checksum
UNION ALL
SELECT * FROM silver
