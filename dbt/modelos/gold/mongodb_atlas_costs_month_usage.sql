SELECT
    DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date
    ,DATE_TRUNC('month', CAST(end_date AS TIMESTAMP)) AS end_date
    ,cluster_name
    ,sku
    ,circle_id
    ,environment
    ,CAST(SUM(total_price_cents) AS decimal(10,2))/100 AS cost
FROM
	{{ ref('mongodb_atlas_costs_usage') }}
GROUP BY
    6,5,4,3,2,1
ORDER BY
    1
