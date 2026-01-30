WITH datadog_complete_data AS (
	SELECT * FROM {{ ref('datadog_costs') }} WHERE invoice_status = 'closed'
	UNION ALL
	SELECT * FROM {{ ref('aux_datadog_costs_backfill') }}
	UNION ALL
	SELECT * FROM {{ ref('aux_datadog_costs_forecast') }}
),

final AS (
    SELECT
		start_date
		,service
		,circle_id
		,circle_type
		,product_name
		,product_group
		,billing_type
		,invoice_status
		,SUM(cost) AS cost
    FROM
        datadog_complete_data
	WHERE
		product_group <> 'shared'
		AND circle_type = 'stream_aligned'
    GROUP BY
        8,7,6,5,4,3,2,1
)

SELECT * FROM final ORDER BY 1 ASC
