WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'fixed_upload_rds_prices') }}
),

final AS (
SELECT
    servicename AS aws_product_name
    ,regioncode AS region
    ,termtype AS contract_term -- OnDemand or Reserved
    ,COALESCE(purchaseoption, 'OnDemand') AS contract_purchase -- No Upfront or OnDemand
    ,COALESCE(leasecontractlength, 'OnDemand') AS contract_length -- 1yr, 3yr or OnDemand
    ,LOWER(REPLACE(databaseengine, ' ', '-')) AS engine
	,LOWER(REPLACE(deploymentoption, ' ', '-')) AS multi_az
    ,instancetype AS instance_size
    ,CASE
        WHEN currentgeneration = 'Yes' THEN true ELSE false
	END AS current_generation
    ,CAST(vcpu AS INTEGER) AS vcpu
    ,CAST(REPLACE(memory, ' GiB', '') AS DOUBLE) AS memory
    ,CAST(priceperunitusd AS DOUBLE) AS cost
    ,storage
FROM source
WHERE
    servicename = 'Amazon Relational Database Service'
    AND locationtype = 'AWS Region'
    AND regioncode IN ('sa-east-1', 'us-east-1')
    AND storage IN ('EBS Only', 'Aurora IO Optimization Mode')
    AND COALESCE(leasecontractlength, 'OnDemand') IN ('1yr', 'OnDemand')
    AND COALESCE(purchaseoption, 'OnDemand') IN ('No Upfront', 'OnDemand')
    AND currentgeneration IS NOT NULL
    AND databaseengine IN ('PostgreSQL', 'MySQL', 'Oracle', 'Aurora MySQL', 'Aurora PostgreSQL')
    AND pricedimensionunit = 'Hrs'
    AND NOT (databaseengine = 'Oracle' AND licensemodel = 'Bring your own license')
)

SELECT 
    aws_product_name
    ,contract_term
    ,contract_purchase
    ,contract_length
    ,region
    ,engine
	,multi_az
    ,instance_size
    ,current_generation
    ,vcpu
    ,memory
    ,cost
    ,storage
 FROM final