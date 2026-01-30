WITH
aws_account_names AS (
    SELECT * FROM {{ ref('aws_account_names') }}
),

aux_rds_instance_commited AS (
    SELECT * FROM {{ ref('aux_rds_instance_commited') }}
),

stg_cloudwatch_rds_report AS (
    SELECT * FROM {{ ref('stg_cloudwatch_rds_report') }}
),

stg_rds_instances_details AS (
    SELECT * FROM {{ ref('stg_rds_instances_details') }}
),

intermediate_aws_cost_usage_report AS (
SELECT * FROM {{ ref('intermediate_aws_cost_usage_report') }}
WHERE (
    aws_product_name = 'Amazon Relational Database Service'
    AND resource_id IS NOT NULL
)
AND (
    usage_type LIKE '%ServerlessV2Usage%'
    OR usage_type LIKE '%ServerlessV2IOOptimizedUsage%'
    OR usage_type LIKE '%InstanceUsage:db.%'
    OR usage_type LIKE '%InstanceUsageIOOptimized:db.%'
    OR usage_type LIKE '%Multi-AZUsage:db.%'
)),

resource_id_details_cw AS (
SELECT
    resource_id
    ,MAX(CASE WHEN usage_type LIKE '%IOOptimized%' THEN 'Aurora IO Optimization Mode' ELSE 'EBS Only' END) OVER (PARTITION BY start_date, resource_id) AS storage
    ,MAX(CASE WHEN tag_product <> 'Not Tagged' THEN tag_product ELSE 'Not Tagged' END) OVER (PARTITION BY start_date, resource_id) AS tag_product
    ,MAX(CASE WHEN tag_management <> 'Not Tagged' THEN tag_management ELSE 'Not Tagged' END) OVER (PARTITION BY start_date, resource_id) AS tag_management
    ,MAX(CASE WHEN tag_repository <> 'Not Tagged' THEN tag_repository ELSE 'Not Tagged' END) OVER (PARTITION BY start_date, resource_id) AS tag_repository
FROM intermediate_aws_cost_usage_report
WHERE 
    start_date >= TIMESTAMP '2025-02-25 19:00:00.0000' 
    AND start_date < TIMESTAMP '2025-05-12 00:00:00.000'
    AND engine = 'aurora-postgresql'
    AND REGEXP_LIKE(instance_type, 'db\.(r6g|r7g|t4g)\.')
),

instances_actives_cw AS (
SELECT
    CAST(start_date AS TIMESTAMP) AS start_date
    ,account_id
    ,region
    ,circle_id
    ,ris.tag_product
    ,ris.tag_management
    ,ris.tag_repository
    ,instance_name
    ,cw.resource_id
    ,true AS status
    ,engine
    ,instance_size
    ,multi_az
    ,COALESCE(NULLIF(ris.storage,''), 'EBS Only') AS storage
FROM stg_cloudwatch_rds_report cw
LEFT JOIN
    resource_id_details_cw ris ON cw.resource_id = ris.resource_id
WHERE CAST(start_date AS TIMESTAMP) >= TIMESTAMP '2025-02-25 19:00:00.0000' 
    AND CAST(start_date AS TIMESTAMP) < TIMESTAMP '2025-05-12 00:00:00.000'
    AND engine = 'aurora-postgresql'
    AND REGEXP_LIKE(instance_size, 'db\.(r6g|r7g|t4g)\.')
GROUP BY 14,13,12,11,10,9,8,7,6,5,4,3,2,1
),

instances_actives_aws AS (
SELECT
    CAST(start_date AS TIMESTAMP) AS start_date
    ,account_id
    ,region
    ,circle_id
    ,tag_product
    ,tag_management
    ,tag_repository
    ,tag_name AS instance_name
    ,resource_id
    ,true AS status
    ,engine
    ,MAX(CASE WHEN usage_type LIKE '%Serverless%' THEN 'db.serverless'
        ELSE instance_type
    END) AS instance_size
    ,MAX(CASE WHEN usage_type LIKE '%Serverless%' THEN 'single-az'
        ELSE multi_az
    END) AS multi_az
    ,MAX(CASE WHEN usage_type LIKE '%IOOptimized%' THEN 'Aurora IO Optimization Mode'
        ELSE 'EBS Only'
    END) AS storage
FROM intermediate_aws_cost_usage_report
WHERE
    CAST(start_date AS TIMESTAMP) < TIMESTAMP '2025-05-12 00:00:00.000'
    AND unblended_cost > 0
    AND NOT (
        CAST(start_date AS TIMESTAMP) >= TIMESTAMP '2025-02-25 19:00:00.000'   
        AND engine = 'aurora-postgresql'
        AND REGEXP_LIKE(instance_type, 'db\.(r6g|r7g|t4g)\.')
    )
GROUP BY 11,10,9,8,7,6,5,4,3,2,1
),

instances_actives_instances_details AS (
SELECT
    CAST(start_date AS TIMESTAMP) AS start_date
    ,account_id
    ,region
    ,circle_id
    ,tag_product
    ,tag_management
    ,tag_repository
    ,instance_name
    ,resource_id
    ,status
    ,engine
    ,instance_size
    ,multi_az
    ,storage
FROM stg_rds_instances_details
WHERE
    CAST(start_date AS TIMESTAMP) >= TIMESTAMP '2025-05-12 00:00:00.000'
GROUP BY 14,13,12,11,10,9,8,7,6,5,4,3,2,1
),

all_instances_actives AS (
SELECT * FROM instances_actives_cw
        UNION ALL
SELECT * FROM instances_actives_aws
        UNION ALL
SELECT * FROM instances_actives_instances_details
),

final AS (
SELECT
    start_date
    ,aic.account_id
    ,account_name
    ,region
    ,COALESCE(MAX(CASE WHEN circle_id <> '999999999999999999999999' THEN circle_id ELSE NULL END) OVER (PARTITION BY start_date, resource_id), '999999999999999999999999') AS circle_id
    ,COALESCE(MAX(CASE WHEN tag_product <> 'Not Tagged' THEN tag_product ELSE NULL END) OVER (PARTITION BY start_date, resource_id), 'Not Tagged') AS tag_product
    ,COALESCE(MAX(CASE WHEN tag_management <> 'Not Tagged' THEN tag_management ELSE NULL END) OVER (PARTITION BY start_date, resource_id), 'Not Tagged') AS tag_management
    ,COALESCE(MAX(CASE WHEN tag_repository <> 'Not Tagged' THEN tag_repository ELSE NULL END) OVER (PARTITION BY start_date, resource_id), 'Not Tagged') AS tag_repository
    ,COALESCE(MAX(CASE WHEN aic.instance_name <> 'Not Tagged' THEN aic.instance_name ELSE NULL END) OVER (PARTITION BY start_date, resource_id), REGEXP_EXTRACT(resource_id, 'db:(.*)', 1)) AS instance_name
    ,resource_id
    ,status
    ,aic.engine
    ,aic.instance_size
    ,multi_az
    ,storage
    ,aric.instance_commited
    ,aric.multiply AS multiply_commited
    ,CASE WHEN region <> 'sa-east-1' OR aic.instance_size = 'db.serverless' OR eligible_commit IS NULL OR storage <> 'EBS Only' THEN false
        ELSE eligible_commit
    END AS eligible_commit
FROM
    all_instances_actives aic
LEFT JOIN
    aws_account_names aan ON aic.account_id = aan.account_id
LEFT JOIN
    aux_rds_instance_commited aric ON aric.instance_size = aic.instance_size AND aric.engine = aic.engine
)

SELECT * FROM final
