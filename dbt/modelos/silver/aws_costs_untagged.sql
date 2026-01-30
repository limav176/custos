WITH
circle AS (
    SELECT * FROM {{ ref('holaspirit_circles') }}
)

, aws AS (
	SELECT
		aws.start_date as start_date
		,aws.resource_id as resource_id
		,aws.description as description
		,aws.billing_origin as billing_origin
		,aws.aws_product_name as aws_product_name
		,'999999999999999999999999' as circle_id
		,aws.providedby_id as providedby_id
		,circle.type as circle_type
		,aws.tag_repository as tag_repository
		,aws.tag_product as tag_product
		,aws.tag_management as tag_management
		,aws.tag_name as tag_name
		,aws.tag_policy_compliant as tag_policy_compliant
		,aws.tag_policy_compliant_actual as tag_policy_compliant_actual
		,aws.environment as environment
		,aws.usage_type as usage_type
		,aws.engine AS engine
		,aws.instance_type AS instance_type
		,aws.pricing_term as pricing_term
		,aws.account_id as account_id
		,aws.account_name as account_name
		,aws.region as region
		,aws.unblended_cost as unblended_cost
		,aws.reservation_cost as reservation_cost
	FROM
		{{ ref('silver_aws_cost_usage_report') }} aws
	LEFT JOIN
		circle circle ON circle.circle_id = aws.circle_id
	WHERE
		NOT COALESCE(aws.billing_origin, '') = 'AWS Marketplace'
		AND COALESCE(aws.aws_product_name, '') <> 'AWS Support (Enterprise)' -- (-) aws_costs_support
		AND NOT(COALESCE(aws.item_type, '') <> 'Usage' AND (COALESCE(aws.circle_id, '999999999999999999999999') = '999999999999999999999999' OR lower(description) LIKE '%tax%' OR lower(description) LIKE '%discount%')) -- (-) aws_costs_shared
		AND COALESCE(aws.circle_id, '999999999999999999999999') = '999999999999999999999999' -- (-) aws_costs_usage
)

SELECT * FROM aws
