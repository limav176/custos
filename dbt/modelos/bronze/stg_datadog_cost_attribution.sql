WITH source AS (
SELECT * FROM {{ source('custos_cloud_bronze', 'datadog_cost_attribution') }}
)

SELECT
    month
    ,tags_service
    ,CASE 
        WHEN LENGTH(tags_circleid) <> 24 THEN NULL 
        ELSE tags_circleid 
    END AS tags_circleid
    ,tags_aws_account
    ,family_type
    ,billing_type
    ,cost
    ,invoice_status
FROM 
    source
WHERE 
    billing_type NOT LIKE '%_checksum'