WITH

data_platform AS (
    SELECT
        CAST(start_date AS DATE) AS start_date
        ,id_resource AS resource_id
        ,id_specific_resource
        ,CASE
            WHEN nr_usage_metric >= 1099511627776 THEN
                CONCAT(CAST(CAST(nr_usage_metric AS DECIMAL(38,2)) / 1099511627776 AS VARCHAR), ' TB')
            WHEN nr_usage_metric >= 1073741824 THEN
                CONCAT(CAST(CAST(nr_usage_metric AS DECIMAL(38,2)) / 1073741824 AS VARCHAR), ' GB')
            WHEN nr_usage_metric >= 1048576 THEN
                CONCAT(CAST(CAST(nr_usage_metric AS DECIMAL(38,2)) / 1048576 AS VARCHAR), ' MB')
            ELSE
                CONCAT(CAST(CAST(nr_usage_metric AS DECIMAL(38,2)) / 1024 AS VARCHAR), ' KB')
        END || ' ' || nm_usage_metric_name AS description
        ,billing_origin
        ,product_name AS aws_product_name
        ,'' AS namespace
        ,'' AS cluster_name
        ,circle_id
        ,providedby_id
        ,circle_type
		,id_specific_resource AS tag_name
		,'' AS tag_repository
		,'' AS tag_product
		,'' AS tag_management
        ,TRUE AS tag_policy_compliant
        ,environment
        ,account_id
        ,account_name
        ,region
        ,SUM(cost) AS cost
        ,SUM(nr_usage_metric) AS nr_datascannedinbytes
    FROM
        {{ ref('data_platform_costs_day_usage') }}
	    --"custos_cloud_gold"."data_platform_costs_day_usage"
    WHERE
        start_date > (current_date - interval '90' day)
    GROUP BY
        20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),

aws AS (
    SELECT
        DATE_TRUNC('day', CAST(a.start_date AS TIMESTAMP)) AS start_date
        ,a.resource_id
        ,a.description
        ,a.billing_origin
        ,a.aws_product_name
        ,'' AS namespace
        ,'' AS cluster_name
        ,a.circle_id
        ,a.providedby_id
        ,a.circle_type
        ,a.tag_name
        ,a.tag_repository
        ,a.tag_product
        ,a.tag_management
        ,a.tag_policy_compliant
        ,a.environment
        ,a.account_id
        ,a.account_name
        ,a.region
        ,CAST(SUM(a.reservation_cost+a.unblended_cost) AS decimal(10,2)) AS cost
        ,0.0 AS nr_datascannedinbytes
    FROM
        {{ ref('aws_costs_usage') }} a
    LEFT JOIN data_platform d ON
        ('aws/' || coalesce(a.account_id, '') || '/' || coalesce(a.region, '') || '/' || coalesce(a.resource_id, '') || '/' || coalesce(a.usage_type, '') || '/' || coalesce(a.pricing_term, '')) = d.resource_id
        AND DATE(a.start_date) = DATE(d.start_date)
    WHERE
        d.resource_id IS NULL
        AND a.start_date > (current_date - interval '90' day)
    GROUP BY
        19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),

eks AS (
    SELECT
        DATE_TRUNC('day', CAST(e.start_date AS TIMESTAMP)) AS start_date
        ,e.instance_id AS resource_id
        ,e.description
        ,e.billing_origin
        ,e.aws_product_name
        ,e.namespace
        ,e.cluster_name
        ,e.circle_id
        ,e.providedby_id
        ,e.circle_type
        ,e.tag_name
        ,e.tag_repository
        ,e.tag_product
        ,e.tag_management
        ,e.tag_policy_compliant
        ,e.environment
        ,e.account_id
        ,e.account_name
        ,e.region
        ,CAST(SUM(e.split_cost + e.unused_cost) AS decimal(10,2)) AS cost
        ,0.0 AS nr_datascannedinbytes
    FROM
        {{ ref('aws_eks_costs_usage') }} e
    LEFT JOIN data_platform d ON
        ('eks/' || coalesce(e.account_id, '') || '/' || coalesce(e.region, '') || '/' || coalesce(e.cluster_name, '') || '/' || coalesce(e.namespace, '')) = d.resource_id
        AND DATE(e.start_date) = DATE(d.start_date)
    WHERE
        d.resource_id IS NULL
        AND e.start_date > (current_date - interval '90' day)
    GROUP BY
        19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
)

SELECT * FROM aws
UNION ALL
SELECT * FROM eks
UNION ALL
SELECT
    start_date
    ,id_specific_resource AS resource_id
    ,description
    ,billing_origin
    ,aws_product_name
    ,namespace
    ,cluster_name
    ,circle_id
    ,providedby_id
    ,circle_type
    ,tag_name
    ,tag_repository
    ,tag_product
    ,tag_management
    ,tag_policy_compliant
    ,environment
    ,account_id
    ,account_name
    ,region
    ,cost
    ,nr_datascannedinbytes
FROM data_platform

