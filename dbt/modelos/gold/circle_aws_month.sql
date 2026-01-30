WITH month AS (
	SELECT start_date, circle_id, environment, account_id, account_name, aws_product_name, sum(cost) as cost
	FROM {{ ref('aws_costs_month_usage') }}
	GROUP BY 1,2,3,4,5,6
)

SELECT
	COALESCE(current_month.circle_id, prev_month.circle_id) as circle_id
	,COALESCE(current_month.environment, prev_month.environment) as environment
	,COALESCE(current_month.account_id, prev_month.account_id) as account_id
	,COALESCE(current_month.account_name, prev_month.account_name) as account_name
	,COALESCE(current_month.aws_product_name, prev_month.aws_product_name) as aws_product_name
    ,COALESCE(current_month.start_date, prev_month.start_date + interval '1' month) as month
	,COALESCE(current_month.cost, 0) as cost
    ,COALESCE(prev_month.start_date, current_month.start_date - interval '1' month)  as previous_month
	,COALESCE(prev_month.cost, 0) as previous_cost
FROM
    month current_month
FULL OUTER JOIN
    month prev_month
ON
	current_month.circle_id = prev_month.circle_id
    AND current_month.environment = prev_month.environment
	AND current_month.account_id = prev_month.account_id
	AND current_month.account_name = prev_month.account_name
	AND current_month.aws_product_name = prev_month.aws_product_name
    AND current_month.start_date - interval '1' month = prev_month.start_date
ORDER BY 1, 6
