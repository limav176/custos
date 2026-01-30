SELECT
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    ,cluster_name
    ,sku
    ,unit
	,SUM(quantity) AS quantity
	,MAX(unit_price_dollars) AS unit_price_dollars
    ,circle_id
    ,tag_policy_compliant
    ,environment
    ,CAST(SUM(total_price_cents) AS decimal(10,2))/100 AS cost
FROM
	{{ ref('mongodb_atlas_costs_usage') }}
GROUP BY
    9,8,7,4,3,2,1
ORDER BY
    1
