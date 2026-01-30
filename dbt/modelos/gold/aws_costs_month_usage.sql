WITH
circle AS (
    SELECT
		circle_id
		,type AS circle_type
	FROM
	    {{ ref('holaspirit_circles') }}
	    --"custos_cloud_silver"."holaspirit_circles"
),
aws AS (
    SELECT
        start_date
        ,environment
        ,account_id
        ,account_name
        ,billing_origin
        ,region
        ,aws_product_name
        ,circle_id
        ,providedby_id
        ,circle_type
        ,unblended_cost + reservation_cost AS cost
    FROM
        {{ ref('aws_costs_usage') }}
	    --"custos_cloud_silver"."aws_costs_usage"
    WHERE
        aws_product_name <> 'Amazon Athena'
),
eks AS (
    SELECT
        start_date
        ,environment
        ,account_id
        ,account_name
        ,billing_origin
        ,region
        ,aws_product_name
        ,circle_id
        ,providedby_id
        ,circle_type
        ,split_cost + unused_cost AS cost
    FROM
        {{ ref('aws_eks_costs_usage') }}
	    --"custos_cloud_silver"."aws_eks_costs_usage"
),
athena AS (
	SELECT
		start_date
		,environment
        ,account_id
        ,account_name
        ,billing_origin
        ,region
        ,aws_product_name
		,a.circle_id
		,providedby_id
		,c.circle_type
		,total_cost AS cost
	FROM
		{{ ref('aws_athena_costs_day_usage') }} a
		--"custos_cloud_gold"."aws_athena_costs_day_usage" a
	LEFT JOIN circle c ON c.circle_id = a.circle_id
),
join_all AS (
    SELECT * FROM aws
     UNION ALL
    SELECT * FROM eks
     UNION ALL
    SELECT * FROM athena
),
final AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date
        ,environment
        ,account_id
        ,account_name
        ,billing_origin
        ,region
        ,aws_product_name
        ,circle_id
        ,providedby_id
        ,circle_type
        ,SUM(cost) AS cost
    FROM
        join_all
    GROUP BY
        10,9,8,7,6,5,4,3,2,1
    ORDER BY
        1
)
SELECT * FROM final
