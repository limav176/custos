SELECT
	start_date
	,end_date
	,clustername as cluster_name
	,sku
	,unit
	,CAST(quantity AS double) as quantity
	,CAST(unitpricedollars AS double) as unit_price_dollars
	,CAST(totalpricecents AS integer) as total_price_cents
	,circle_id
	,tag_policy_compliant
	,environment
FROM
	{{ ref('silver_mongodb_atlas_cost_usage_report') }}
WHERE
	sku = 'ATLAS_SUPPORT'
