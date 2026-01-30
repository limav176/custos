WITH
aws AS (
    SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
        ,resource_id
        ,description
        ,billing_origin
        ,aws_product_name
        ,circle_id
        ,providedby_id
        ,circle_type
		,tag_repository
		,tag_product
		,tag_management
		,tag_name
        ,tag_policy_compliant
        ,tag_policy_compliant_actual
        ,environment
        ,usage_type
        ,pricing_term
        ,account_id
        ,account_name
        ,region
        ,SUM(reservation_cost) AS reservation_cost
        ,SUM(unblended_cost) AS unblended_cost
    FROM
        {{ ref('aws_costs_untagged') }}
    GROUP BY
        20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
)

SELECT * FROM aws
