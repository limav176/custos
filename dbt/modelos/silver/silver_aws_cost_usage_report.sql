WITH aws AS (
SELECT
    start_date
    ,resource_id
    ,description
    ,aws_product_name
    ,circle_id
    ,providedby_id
    ,circle_id_source
    ,tag_repository
    ,tag_product
    ,tag_management
    ,tag_name
    ,tag_policy_compliant
    ,tag_policy_compliant_actual
    ,billing_origin
    ,item_type
    ,account_id
    ,account_name
    ,region
    ,environment
    ,usage_type
    ,instance_type
    ,multi_az
    ,engine
    ,pricing_term
    ,unblended_cost
    ,reservation_cost
    ,reservation_start_time
    ,reservation_end_time
    ,reservation_net_effective_cost
    ,reservation_amortized_upfront_cost
FROM {{ ref('intermediate_aws_cost_usage_report') }} 
WHERE COALESCE(aws_product_name,'') NOT IN ('Amazon Relational Database Service', 'Crédito AWS - Incentivo SES')
),

rds AS (
SELECT
    start_date
    ,resource_id
    ,description
    ,aws_product_name
    ,circle_id
    ,providedby_id
    ,circle_id_source
    ,tag_repository
    ,tag_product
    ,tag_management
    ,tag_name
    ,tag_policy_compliant
    ,tag_policy_compliant_actual
    ,billing_origin
    ,item_type
    ,account_id
    ,account_name
    ,region
    ,environment
    ,usage_type
    ,instance_type
    ,multi_az
    ,engine
    ,pricing_term
    ,unblended_cost
    ,reservation_cost
    ,reservation_start_time
    ,reservation_end_time
    ,reservation_net_effective_cost
    ,reservation_amortized_upfront_cost
FROM {{ ref('silver_aws_rds_costs') }} 
),

final AS (
SELECT * FROM aws
    UNION ALL 
SELECT * FROM rds
)

SELECT * FROM final