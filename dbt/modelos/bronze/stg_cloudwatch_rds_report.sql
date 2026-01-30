WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'cloudwatch_rds_report') }}
),

final AS (
SELECT
    CAST(start_date AS TIMESTAMP) AS start_date
    ,account_id
    ,region
    ,circle_id
    ,instance_name
    ,resource_id
    ,instance_size
    ,CASE WHEN engine = 'postgres' THEN 'postgresql'
         WHEN engine = 'oracle-se2' THEN 'oracle'
         ELSE LOWER(REPLACE(engine, ' ', '-'))
    END AS engine
    ,LOWER(REPLACE(multi_az, ' ', '-')) AS multi_az
    ,metric_name
    ,metric_statistics
    ,CAST(metric_value AS DOUBLE) AS metric_value
FROM source
)

SELECT 
    start_date
    ,account_id
    ,region
    ,CASE 
        WHEN LENGTH(circle_id) <> 24 THEN NULL 
        ELSE circle_id 
    END AS circle_id
    ,instance_name
    ,resource_id
    ,instance_size
    ,engine
    ,multi_az
    ,metric_name
    ,metric_statistics
    ,metric_value
FROM 
    final