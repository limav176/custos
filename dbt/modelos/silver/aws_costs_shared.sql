SELECT
	aws.start_date
	,aws.resource_id
	,aws.description
	,aws.billing_origin
	,aws.aws_product_name
	,aws.circle_id
	,aws.providedby_id
	,aws.tag_repository
	,aws.tag_product
	,aws.tag_management
	,aws.tag_name
	,aws.environment
	,aws.usage_type
	,aws.engine
	,aws.instance_type
	,aws.pricing_term
	,aws.account_id
	,aws.account_name
	,aws.region
	,aws.unblended_cost
	,aws.reservation_cost
FROM
	{{ ref('silver_aws_cost_usage_report') }} aws
WHERE
	NOT COALESCE(aws.billing_origin, '') = 'AWS Marketplace'
	AND COALESCE(aws.aws_product_name, '') <> 'AWS Support (Enterprise)'
	AND COALESCE(aws.item_type, '') <> 'Usage' AND (COALESCE(aws.circle_id, '999999999999999999999999') = '999999999999999999999999' OR lower(description) LIKE '%tax%' OR lower(description) LIKE '%discount%')