WITH forecast as (
	select
		origin
		,n1_circle_name
		,month
		,cost
		,previous_month
		,previous_cost
	from {{ ref('circle_forecast_month') }} fo
),

actual as (
	select
		origin
		,c.n1_circle_name
		,month
		,sum(cost) as cost
		,previous_month
		,sum(previous_cost) as previous_cost
	from {{ ref('circle_month') }} fo
	left join {{ ref('circles') }} c
		on fo.circle_id = c.circle_id
	group by 1,2,3,5
)

SELECT
	COALESCE(a.origin, f.origin) as origin
	,COALESCE(a.n1_circle_name, f.n1_circle_name) as n1_circle_name
    ,COALESCE(a.month, f.month) as month
	,COALESCE(a.cost, 0) as actual_cost
	,COALESCE(f.cost, 0) as forecast_cost
    ,COALESCE(a.previous_month, f.previous_month) as previous_month
	,COALESCE(a.previous_cost, 0) as actual_previous_cost
	,COALESCE(f.previous_cost, 0) as forecast_previous_cost
FROM
    forecast f
FULL OUTER JOIN
    actual a
ON
    a.origin = f.origin
	AND a.n1_circle_name = f.n1_circle_name
    AND a.month = f.month
    AND a.previous_month = f.previous_month
ORDER BY
    3,1,2 DESC



