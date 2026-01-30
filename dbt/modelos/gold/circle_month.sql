WITH month AS (
	SELECT * FROM {{ ref('circle_temp_2') }}
)

SELECT
	COALESCE(current_month.origin, prev_month.origin) as origin
	,COALESCE(current_month.circle_id, prev_month.circle_id) as circle_id
	,COALESCE(current_month.type, prev_month.type) as type
    ,COALESCE(current_month.start_date, prev_month.start_date + interval '1' month) as month
	,COALESCE(current_month.cost, 0) as cost
    ,COALESCE(prev_month.start_date, current_month.start_date - interval '1' month)  as previous_month
	,COALESCE(prev_month.cost, 0) as previous_cost
FROM
    month current_month
FULL OUTER JOIN
    month prev_month
ON
    current_month.origin = prev_month.origin
	AND current_month.circle_id = prev_month.circle_id
	AND current_month.type = prev_month.type
    AND current_month.start_date - interval '1' month = prev_month.start_date
ORDER BY
    3,1 DESC
