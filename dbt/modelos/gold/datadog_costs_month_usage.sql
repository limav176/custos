WITH
final AS (
    SELECT
		start_date
		,service
		,circle_id
		,circle_type
		,product_name as original_product_name
		,product_name
		,product_group
		,billing_type
        ,invoice_status
		,SUM(cost) AS cost
    FROM
        {{ ref('datadog_costs_usage') }}
    GROUP BY
        9,8,7,6,5,4,3,2,1
)

SELECT * FROM final ORDER BY 1 ASC
