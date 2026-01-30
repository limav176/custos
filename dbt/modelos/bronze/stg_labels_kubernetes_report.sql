WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'labels_kubernetes_report') }}
)

SELECT 
    namespace
    ,CASE 
        WHEN LENGTH(circleid) <> 24 THEN NULL 
        ELSE circleid 
    END AS circleid
    ,cluster_name
FROM source