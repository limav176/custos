WITH circles AS (
	SELECT * FROM {{ ref('holaspirit_circles') }} c
)
,circles_with_usage AS(
  SELECT DISTINCT
  	c.circle_id
  	,c.circle_name
  	,c.n1_circle_name
  	,c.type
  FROM
  	circles c
  INNER JOIN
  	{{ ref('aws_costs_month_usage') }} u
  ON c.circle_id = u.circle_id
  WHERE c.circle_id <> '62212de0578581684e464069'
)
,n1 as (
  SELECT DISTINCT
  	c.circle_id
  	,c.circle_name
  	,c.n1_circle_name
  	,c.type
  FROM
  	circles c
  WHERE c.circle_name=c.n1_circle_name
  AND c.circle_id not in ('62212de0578581684e464069','999999999999999999999999') 
)
SELECT * from circles_with_usage
UNION
SELECT * from n1
ORDER BY 3 DESC
