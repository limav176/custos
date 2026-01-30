WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'mongodb_atlas_cost_usage_report') }}
),

final AS (
    SELECT
        DATE_TRUNC('month', CAST(PARSE_DATETIME(created, 'yyyy-MM-dd''T''HH:mm:ss''Z') AS TIMESTAMP)) AS start_date
        ,'mongodb_atlas_cost_usage_report' AS table_name
        ,sku
        ,COALESCE(CAST(json_extract(replace(tags, '''', '"'), '$.CircleId[0]') AS varchar), 'unknown_circle') AS circle_id
        ,SUM(CAST(quantity AS double)) AS quantity
        ,CAST(SUM(CAST(totalpricecents AS DECIMAL(10,2))) / 100 AS DECIMAL(10,2)) AS cost
    FROM
        source
    GROUP BY 4,3,2,1
)

SELECT
    start_date
    ,table_name
    ,sku
    ,CASE 
        WHEN LENGTH(circle_id) <> 24 THEN NULL 
        ELSE circle_id 
    END AS circle_id
    ,quantity
    ,cost
FROM final
ORDER BY start_date
