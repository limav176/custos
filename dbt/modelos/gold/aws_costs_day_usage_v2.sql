WITH

data_platform AS (
    SELECT
        CAST(start_date AS DATE) AS start_date
        ,id_resource AS resource_id
        ,id_specific_resource
        ,CAST(nr_usage_metric AS VARCHAR) || ' ' || nm_usage_metric_name AS description
        ,billing_origin
        ,product_name AS aws_product_name
        ,circle_id
        ,providedby_id
        ,circle_type
		,'' AS tag_repository
		,'' AS tag_product
		,'' AS tag_management
		,id_specific_resource AS tag_name
        ,TRUE AS tag_policy_compliant
        ,TRUE AS tag_policy_compliant_actual
        ,environment
        ,'' AS usage_type
        ,'' AS pricing_term
        ,account_id
        ,account_name
        ,region
        ,0.0 AS reservation_cost
        ,SUM(cost) AS unblended_cost
    FROM
        {{ ref('data_platform_costs_day_usage') }}
	    --"custos_cloud_gold"."data_platform_costs_day_usage"
    WHERE
        id_resource like 'aws/%'
    GROUP BY
        22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),

aws AS (
    SELECT
        DATE_TRUNC('day', CAST(a.start_date AS TIMESTAMP)) AS start_date
        ,a.resource_id
        ,a.description
        ,a.billing_origin
        ,a.aws_product_name
        ,a.circle_id
        ,a.providedby_id
        ,a.circle_type
		,a.tag_repository
		,a.tag_product
		,a.tag_management
		,a.tag_name
        ,a.tag_policy_compliant
        ,a.tag_policy_compliant_actual
        ,a.environment
        ,a.usage_type
        ,a.pricing_term
        ,a.account_id
        ,a.account_name
        ,a.region
        ,SUM(a.reservation_cost) AS reservation_cost
        ,SUM(a.unblended_cost) AS unblended_cost
    FROM
        {{ ref('aws_costs_usage') }} a
	    --"custos_cloud_silver"."aws_costs_usage"
    LEFT JOIN data_platform d ON
        ('aws/' || coalesce(a.account_id, '') || '/' || coalesce(a.region, '') || '/' || coalesce(a.resource_id, '') || '/' || coalesce(a.usage_type, '') || '/' || coalesce(a.pricing_term, '')) = d.resource_id
        AND DATE(a.start_date) = DATE(d.start_date)
    WHERE d.resource_id IS NULL
    GROUP BY
        20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
)

SELECT * FROM aws
UNION ALL
SELECT
    start_date
    ,id_specific_resource AS resource_id
    ,description
    ,billing_origin
    ,aws_product_name
    ,circle_id
    ,providedby_id
    ,circle_type
    ,tag_repository
    ,tag_product
    ,tag_management
    ,tag_name
    ,tag_policy_compliant
    ,tag_policy_compliant_actual
    ,environment
    ,usage_type
    ,pricing_term
    ,account_id
    ,account_name
    ,region
    ,reservation_cost
    ,unblended_cost
FROM data_platform
