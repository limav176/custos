WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'aws_cost_usage_report') }}
    WHERE line_item_operation = 'EKSPod-EC2' AND line_item_line_item_type <> 'DiscountedUsage'
),

staging AS (
SELECT 
    line_item_usage_start_date AS start_date
    ,line_item_resource_id AS resource_id
    ,CONCAT(
        REPLACE(
            SPLIT_PART(line_item_resource_id, '/', 1)
            ,'pod'
            ,'cluster'
        )
        ,'/'
        ,SPLIT_PART(line_item_resource_id, '/', 2)
    ) AS cluster_arn
    ,split_line_item_parent_resource_id as parent_resource_id
    ,SPLIT_PART(line_item_resource_id, '/', 3) AS namespace
    ,bill_billing_entity AS billing_origin
    ,line_item_usage_account_id AS account_id
    ,product_region AS region
    ,line_item_usage_type AS usage_type
    ,pricing_term AS pricing_term
    ,'Kubernetes' AS aws_product_name
    ,split_line_item_split_cost AS split_cost
    ,split_line_item_unused_cost AS unused_cost
FROM source
),

final as (
SELECT 
    start_date
    ,resource_id
    ,cluster_arn
    ,SPLIT_PART(cluster_arn, '/', 2) AS cluster_name
    ,parent_resource_id
    ,namespace
    ,billing_origin
    ,account_id
    ,region
    ,usage_type
    ,pricing_term
    ,aws_product_name
    ,split_cost
    ,unused_cost
FROM staging
)

SELECT 
    start_date
    ,resource_id
    ,cluster_arn
    ,cluster_name
    ,parent_resource_id
    ,namespace
    ,billing_origin
    ,account_id
    ,region
    ,usage_type
    ,pricing_term
    ,aws_product_name
    ,split_cost
    ,unused_cost
FROM final