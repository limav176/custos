WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'aws_cost_usage_report') }}
    WHERE line_item_operation <> 'EKSPod-EC2' AND line_item_line_item_type <> 'DiscountedUsage'
),

final AS (
SELECT
    CAST(line_item_usage_start_date AS TIMESTAMP) AS start_date
    ,NULLIF(line_item_resource_id, '') AS resource_id
    ,NULLIF(line_item_line_item_description, '') AS description
    ,CASE WHEN (resource_tags_user_repository LIKE '%/airflow-dags' AND product_product_name='Amazon Elastic Compute Cloud') THEN 'EC2 EMR' -- untagged EMR
        WHEN (line_item_line_item_description in ('Sales PoC - Geos_Sales LATAM (phase2), credit from account: 142401413602','WILL - Poc Data, credit from account: 142401413602')) THEN 'Crédito AWS - Incentivo SES'
        WHEN (line_item_line_item_description like 'SavingsPlanNegation%') THEN 'Savings Plan' -- Savings Plan, posteriormente mover para tech plat
        ELSE NULLIF(product_product_name, '')
    END AS aws_product_name
    ,NULLIF(resource_tags_user_circle_id, '') AS circle_id
    ,NULLIF(resource_tags_user_circle, '') AS tag_circle
    ,NULLIF(resource_tags_user_provided_by_id,'') AS providedby_id
    ,NULLIF(resource_tags_user_repository, '') AS tag_repository
    ,NULLIF(resource_tags_user_product, '') AS tag_product
    ,NULLIF(resource_tags_user_management, '') AS tag_management
    ,CASE WHEN (resource_tags_user_repository LIKE '%/airflow-dags' AND product_product_name='Amazon Elastic Compute Cloud') THEN 'EMR' -- untagged EMR
        ELSE NULLIF(resource_tags_user_name, '')
    END AS tag_name
    ,NULLIF(bill_billing_entity, '') AS billing_origin
    ,NULLIF(line_item_line_item_type, '') AS item_type
    ,NULLIF(line_item_operation, '') AS item_operation
    ,NULLIF(line_item_usage_account_id, '') AS account_id
    ,NULLIF(product_region, '') AS region
    ,NULLIF(line_item_usage_type, '') AS usage_type
    ,CASE WHEN product_product_name = 'Amazon Relational Database Service' AND line_item_usage_type LIKE '%Serverless%' AND NULLIF(product_instance_type, '') IS NULL THEN 'db.serverless'
        ELSE NULLIF(product_instance_type, '')
    END AS instance_type
    ,CASE WHEN product_product_name = 'Amazon Relational Database Service' AND line_item_usage_type LIKE '%Serverless%' AND NULLIF(product_deployment_option, '') IS NULL THEN 'single-az'
        ELSE LOWER(NULLIF(product_deployment_option, ''))
    END AS multi_az
    ,LOWER(REPLACE(NULLIF(product_database_engine,''), ' ', '-')) AS engine
    ,NULLIF(pricing_term, '') AS pricing_term
    ,TRY(CAST(from_iso8601_timestamp(NULLIF(reservation_start_time, '')) AS TIMESTAMP)) AS reservation_start_time
    ,TRY(CAST(from_iso8601_timestamp(NULLIF(reservation_end_time, '')) AS TIMESTAMP)) AS reservation_end_time
    ,reservation_reservation_a_r_n AS reservation_arn
    ,TRY_CAST(reservation_subscription_id AS BIGINT) AS reservation_id
    ,TRY_CAST(reservation_number_of_reservations AS INT) AS reservations
    ,TRY_CAST(reservation_units_per_reservation AS DOUBLE) AS reservation_hours
    ,REGEXP_EXTRACT(line_item_usage_type, ':(.*)', 1) AS instance_commited
    ,pricing_lease_contract_length AS contract_length
    ,pricing_offering_class AS offering_class
    ,pricing_purchase_option AS contract_purchase
    ,line_item_unblended_cost AS unblended_cost
    ,reservation_effective_cost AS reservation_cost
    ,reservation_net_effective_cost AS reservation_net_effective_cost
    ,reservation_amortized_upfront_cost_for_usage AS reservation_amortized_upfront_cost
FROM source
)

SELECT 
    start_date
    ,resource_id
    ,description
    ,aws_product_name
    ,CASE 
        WHEN LENGTH(circle_id) <> 24 THEN NULL 
        ELSE circle_id 
    END AS circle_id
    ,tag_circle
    ,providedby_id
    ,tag_repository
    ,tag_product
    ,tag_management
    ,tag_name
    ,CASE
        WHEN aws_product_name LIKE '%Bedrock%' THEN 'AWS'
        WHEN aws_product_name IN ('OCBAWS Brazil 2P') THEN 'AWS Marketplace'
        WHEN description IN ('Contractual Credit - Credits, credit from account: 887592687927') THEN 'AWS Marketplace'
        ELSE billing_origin
    END AS billing_origin
    ,item_type
    ,item_operation
    ,account_id
    ,region
    ,usage_type
    ,instance_type
    ,multi_az
    ,engine
    ,pricing_term
    ,reservation_start_time
    ,reservation_end_time
    ,reservation_arn
    ,reservation_id
    ,reservations
    ,reservation_hours
    ,instance_commited
    ,contract_length
    ,offering_class
    ,contract_purchase
    ,unblended_cost
    ,reservation_cost
    ,reservation_net_effective_cost
    ,reservation_amortized_upfront_cost
FROM 
	final
