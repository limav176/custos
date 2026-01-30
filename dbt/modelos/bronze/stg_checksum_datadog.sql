WITH 

checksum_from_api AS (
    SELECT
        DATE_TRUNC('month', CAST(month AS TIMESTAMP)) AS start_date
        ,'checksum_from_api' as table_name
        ,tags_circleid as circle_id
        ,family_type as product_name
        ,billing_type as billing_type
        ,SUM(CAST(cost AS DOUBLE)) AS cost
    FROM
        {{ source('custos_cloud_bronze', 'datadog_cost_attribution') }}
    WHERE
        billing_type IN ('on_demand_checksum', 'committed_checksum')
        AND CAST(cost AS DOUBLE) > 0.0
    GROUP BY 5,4,3,2,1
),

stg_datadog AS (
    SELECT
        DATE_TRUNC('month', CAST(month AS TIMESTAMP)) AS start_date
        ,'datadog_cost_attribution' as table_name
        ,tags_circleid as circle_id
        ,family_type as product_name
        ,billing_type as billing_type
        ,SUM(CAST(cost AS DOUBLE)) AS cost
    FROM
        {{ source('custos_cloud_bronze', 'datadog_cost_attribution') }}
    WHERE
        billing_type <> 'total'
        AND billing_type NOT LIKE '%_checksum'
        AND family_type NOT LIKE '%_cost_sum'
        AND CAST(cost AS DOUBLE) > 0.0
    GROUP BY 5,4,3,2,1
),

stg_checksum AS (
    SELECT * FROM checksum_from_api
    UNION ALL
    SELECT * FROM stg_datadog
)

SELECT 
    start_date
    ,table_name
    ,CASE 
        WHEN LENGTH(circle_id) <> 24 THEN NULL 
        ELSE circle_id 
    END AS circle_id
    ,product_name
    ,billing_type
    ,cost
FROM stg_checksum