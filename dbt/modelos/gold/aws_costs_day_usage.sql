WITH
circle AS (
    SELECT
		circle_id
		,type AS circle_type
	FROM
	    {{ ref('holaspirit_circles') }}
	    --"custos_cloud_silver"."holaspirit_circles"
),
aws AS (
    SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
        ,resource_id
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
        ,SUM(reservation_cost) AS reservation_cost
        ,SUM(unblended_cost) AS unblended_cost
    FROM
        {{ ref('aws_costs_usage') }}
	    --"custos_cloud_silver"."aws_costs_usage"
    WHERE
        aws_product_name <> 'Amazon Athena'
    GROUP BY
        20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
athena AS (
	SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
        ,workgroup AS resource_id
        ,'' AS description
        ,billing_origin
        ,aws_product_name
        ,a.circle_id
        ,providedby_id
        ,c.circle_type
		,'' AS tag_repository
		,'' AS tag_product
		,'' AS tag_management
		,workgroup AS tag_name
        ,TRUE AS tag_policy_compliant
        ,TRUE AS tag_policy_compliant_actual
        ,environment
        ,'' AS usage_type
        ,'' AS pricing_term
        ,account_id
        ,account_name
        ,region
        ,0.0 AS reservation_cost
        ,SUM(total_cost) AS unblended_cost
	FROM
		{{ ref('aws_athena_costs_day_usage') }} a
		--"custos_cloud_gold"."aws_athena_costs_day_usage" a
	LEFT JOIN
	    circle c ON c.circle_id = a.circle_id
    GROUP BY
        21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
)

SELECT * FROM aws
UNION ALL
SELECT * FROM athena
