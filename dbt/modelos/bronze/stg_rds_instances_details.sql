WITH
source AS (
SELECT * FROM {{ source('custos_cloud_bronze', 'rds_instances_details') }}
),

tag AS (
SELECT
    start_date,
    dbinstancearn,
    TRY_CAST(
      json_parse(REPLACE(taglist, '''', '"')) AS ARRAY(JSON)
    ) AS taglist
FROM
    source
WHERE taglist IS NOT NULL
GROUP BY 3,2,1
),

final AS (
SELECT
    CAST(s.start_date AS TIMESTAMP) AS start_date
    ,awsaccountid AS account_id
    ,region
    ,CASE WHEN engine = 'postgres' THEN 'postgresql'
         WHEN engine = 'oracle-se2' THEN 'oracle'
         ELSE LOWER(REPLACE(engine, ' ', '-'))
    END AS engine
    ,CASE WHEN
        dbinstancestatus IN (
            'available', 'starting', 'modifying', 'rebooting', 'backing-up', 'maintenance', 'upgrading',
            'storage-optimization', 'configuring-log-exports', 'configuring-enhanced-monitoring', 'configuring-iam-database-auth'
        ) THEN TRUE
        ELSE FALSE
    END AS status
    ,CAST(instancecreatetime AS TIMESTAMP) AS instance_created_time
    ,dbinstanceidentifier AS instance_name
    ,s.dbinstancearn AS resource_id
    ,dbinstanceclass AS instance_size
    ,CASE WHEN multiaz = TRUE THEN 'multi-az' ELSE 'single-az' END AS multi_az
    ,licensemodel AS instance_license
    ,storagetype AS storage_type
    ,element_at(
        transform(
            filter(
                tag.taglist,
                json_obj -> json_extract_scalar(json_obj, '$.Key') = 'CircleId'
            ),
        json_obj -> json_extract_scalar(json_obj, '$.Value')
        ),
    1
    ) AS circle_id
    ,element_at(
        transform(
            filter(
                tag.taglist,
                json_obj -> json_extract_scalar(json_obj, '$.Key') = 'Product'
            ),
        json_obj -> json_extract_scalar(json_obj, '$.Value')
        ),
    1
    ) AS tag_product
    ,element_at(
        transform(
            filter(
                tag.taglist,
                json_obj -> json_extract_scalar(json_obj, '$.Key') = 'Management'
            ),
        json_obj -> json_extract_scalar(json_obj, '$.Value')
        ),
    1
    ) AS tag_management
    ,element_at(
        transform(
            filter(
                tag.taglist,
                json_obj -> json_extract_scalar(json_obj, '$.Key') = 'Repository'
            ),
        json_obj -> json_extract_scalar(json_obj, '$.Value')
        ),
    1
  ) AS tag_repository
  ,CASE WHEN storagetype = 'aurora-iopt1' THEN 'Aurora IO Optimization Mode'
        ELSE 'EBS Only'
    END AS storage
FROM source s
LEFT JOIN tag tag ON tag.dbinstancearn = s.dbinstancearn AND tag.start_date = s.start_date
)

SELECT 
    start_date
    ,account_id
    ,region
    ,engine
    ,status
    ,instance_created_time
    ,instance_name
    ,resource_id
    ,instance_size
    ,multi_az
    ,instance_license
    ,storage_type
    ,CASE 
        WHEN LENGTH(circle_id) <> 24 THEN NULL 
        ELSE circle_id 
    END AS circle_id
    ,tag_product
    ,tag_management
    ,tag_repository
    ,storage
FROM final