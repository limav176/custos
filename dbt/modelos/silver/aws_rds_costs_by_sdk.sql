WITH
stg_aws_rds_prices AS (
    SELECT * FROM {{ ref('stg_aws_rds_prices') }}
),

aws_rds_instances_details AS (
    SELECT * FROM {{ ref('aws_rds_instances_details') }} WHERE status = TRUE
),

aws AS (
SELECT
    start_date
    ,resource_id
    ,usage_type
    ,CAST(SUM(reservation_cost+unblended_cost) AS DOUBLE) AS cost
FROM
    {{ ref('intermediate_aws_cost_usage_report') }}
WHERE
    aws_product_name = 'Amazon Relational Database Service'
    AND resource_id IS NOT NULL
    AND (
        usage_type LIKE '%ServerlessV2Usage%'
        OR usage_type LIKE '%ServerlessV2IOOptimizedUsage%'
        OR usage_type LIKE '%ExtendedSupport%'
    )
GROUP BY 3,2,1
),

size_price AS (
SELECT
    region
    ,instance_size
    ,engine
    ,multi_az
    ,vcpu
    ,memory
    ,storage
    ,SUM(CASE
        WHEN contract_term = 'Reserved' AND contract_length = '1yr' AND contract_purchase = 'No Upfront'
        THEN CAST(cost AS DOUBLE)
        ELSE 0.0
    END) AS reserved_cost
    ,SUM(CASE
         WHEN contract_term = 'OnDemand'
         THEN CAST(cost AS DOUBLE)
         ELSE 0.0
    END) AS ondemand_cost
FROM
    stg_aws_rds_prices
WHERE
    engine IN ('mysql', 'postgresql', 'aurora-postgresql', 'aurora-mysql', 'oracle')
    AND multi_az IN ('single-az', 'multi-az')
    AND contract_purchase IN ('OnDemand', 'No Upfront')
    AND contract_length IN ('1yr', 'OnDemand')
    AND storage IN ('EBS Only', 'Aurora IO Optimization Mode')
GROUP BY 7,6,5,4,3,2,1
),

costs_from_serverless_and_extended_support AS (
SELECT
    arid.start_date
    ,arid.account_id
    ,arid.account_name
    ,arid.region
    ,arid.circle_id
    ,arid.tag_repository
    ,arid.tag_product
    ,arid.tag_management
    ,arid.instance_name
    ,arid.resource_id
    ,arid.instance_size
    ,arid.instance_commited
    ,arid.multiply_commited
    ,arid.eligible_commit
    ,arid.engine
    ,arid.multi_az
    ,arid.storage
    ,CASE
        WHEN usage_type LIKE '%Serverless%' THEN 'Usage'
        WHEN usage_type LIKE '%ExtendedSupport%' THEN 'Extended Support'
        ELSE NULL END
    AS usage_type
    ,NULL AS vcpu
    ,NULL AS memory
    ,0.0 AS opportunity_savings
    ,CAST(aws.cost AS DOUBLE) AS cost
FROM
    aws_rds_instances_details arid
INNER JOIN aws aws ON
        aws.start_date = arid.start_date
    AND aws.resource_id = arid.resource_id
ORDER BY 1 ASC
),

costs_from_size AS (
SELECT
    arid.start_date
    ,arid.account_id
    ,arid.account_name
    ,arid.region
    ,arid.circle_id
    ,arid.tag_repository
    ,arid.tag_product
    ,arid.tag_management
    ,arid.instance_name
    ,arid.resource_id
    ,arid.instance_size
    ,arid.instance_commited
    ,arid.multiply_commited
    ,arid.eligible_commit
    ,arid.engine
    ,arid.multi_az
    ,arid.storage
    ,'Usage' AS usage_type
    ,sz.vcpu
    ,sz.memory
    ,CASE WHEN COALESCE(sz.reserved_cost,0.0) = 0.0 THEN 0.0
        ELSE CAST(sz.ondemand_cost-sz.reserved_cost AS DOUBLE)
    END AS opportunity_savings
    ,CAST(sz.ondemand_cost AS DOUBLE) AS cost
FROM
    aws_rds_instances_details arid
LEFT JOIN size_price sz ON
        sz.region = arid.region
    AND sz.engine = arid.engine
    AND sz.instance_size = arid.instance_size
    AND sz.multi_az = arid.multi_az
    AND sz.storage = arid.storage
WHERE
    arid.instance_size <> 'db.serverless'
ORDER BY 1 ASC
),

union_all AS (
SELECT * FROM costs_from_serverless_and_extended_support
        UNION ALL
SELECT * FROM costs_from_size
),

final AS (
SELECT
    start_date
    ,account_id
    ,account_name
    ,region
    ,circle_id
    ,tag_repository
    ,tag_product
    ,tag_management
    ,instance_name
    ,resource_id
    ,instance_size
    ,instance_commited
    ,multiply_commited
    ,eligible_commit
    ,engine
    ,multi_az
    ,storage
    ,usage_type
    ,vcpu
    ,memory
    ,CAST(SUM(opportunity_savings) AS DOUBLE) AS opportunity_savings
    ,CAST(SUM(cost) AS DOUBLE) AS total_cost
FROM
    union_all
GROUP BY 20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
ORDER BY 1 ASC
)

SELECT * FROM final
