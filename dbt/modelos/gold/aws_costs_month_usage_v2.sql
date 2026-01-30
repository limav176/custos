WITH

data_platform AS (
    SELECT
        start_date
        ,id_resource AS resource_id
        ,environment
        ,account_id
        ,account_name
        ,billing_origin
        ,region
        ,product_name AS aws_product_name
        ,circle_id
        ,providedby_id
        ,circle_type
        ,cost
    FROM
        {{ ref('data_platform_costs_day_usage') }}
	    --"custos_cloud_gold"."data_platform_costs_day_usage"
),

aws AS (
    SELECT
        a.start_date
        ,a.environment
        ,a.account_id
        ,a.account_name
        ,a.billing_origin
        ,a.region
        ,a.aws_product_name
        ,a.circle_id
        ,a.providedby_id
        ,a.circle_type
        ,a.unblended_cost + a.reservation_cost AS cost
    FROM
        {{ ref('aws_costs_usage') }} a
	    --"custos_cloud_silver"."aws_costs_usage"
    LEFT JOIN data_platform d ON
        ('aws/' || coalesce(a.account_id, '') || '/' || coalesce(a.region, '') || '/' || coalesce(a.resource_id, '') || '/' || coalesce(a.usage_type, '') || '/' || coalesce(a.pricing_term, '')) = d.resource_id
        AND DATE(a.start_date) = DATE(d.start_date)
    WHERE d.resource_id IS NULL
),

eks AS (
    SELECT
        e.start_date
        ,e.environment
        ,e.account_id
        ,e.account_name
        ,e.billing_origin
        ,e.region
        ,e.aws_product_name
        ,e.circle_id
        ,e.providedby_id
        ,e.circle_type
        ,e.split_cost + e.unused_cost AS cost
    FROM
        {{ ref('aws_eks_costs_usage') }} e
	    --"custos_cloud_silver"."aws_eks_costs_usage"
    LEFT JOIN data_platform d ON
        ('eks/' || coalesce(e.account_id, '') || '/' || coalesce(e.region, '') || '/' || coalesce(e.cluster_name, '') || '/' || coalesce(e.namespace, '')) = d.resource_id
        AND DATE(e.start_date) = DATE(d.start_date)
    WHERE d.resource_id IS NULL
),

join_all AS (
    SELECT * FROM aws
     UNION ALL
    SELECT * FROM eks
     UNION ALL
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
        ,cost
    FROM data_platform
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
