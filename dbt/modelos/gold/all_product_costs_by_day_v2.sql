WITH

data_platform AS (
    SELECT
        start_date
        ,'data_platform' AS provider
        ,account_name
        ,circle_id
        ,product_name
        ,id_resource AS resource_id
        ,id_specific_resource AS tag_name
        ,'data_platform_service' AS resource_type
        ,SUM(cost) AS total_cost
    FROM
        {{ ref('data_platform_costs_day_usage') }}
	    --"custos_cloud_gold"."data_platform_costs_day_usage"
    GROUP BY
        8,7,6,5,4,3,2,1
),

eks AS (
    SELECT
        CAST(e.start_date AS DATE) AS start_date
        ,'kubernetes' AS provider
        ,e.account_name
        ,e.circle_id
        ,'kubernetes' AS product_name
        ,e.namespace AS resource_id
        ,e.namespace AS tag_name
        ,'kubernetes_namespace' AS resource_type
        ,SUM(e.split_cost + e.unused_cost) AS total_cost
    FROM
        {{ ref('aws_eks_costs_day_usage') }} e
    LEFT JOIN data_platform d ON
        ('eks/' || coalesce(e.account_id, '') || '/' || coalesce(e.region, '') || '/' || coalesce(e.cluster_name, '') || '/' || coalesce(e.namespace, '')) = d.resource_id
        AND DATE(e.start_date) = DATE(d.start_date)
    WHERE d.resource_id IS NULL
    GROUP BY
        8,7,6,5,4,3,2,1
),

aws_costs AS (
    SELECT
        CAST(a.start_date AS DATE) AS start_date
        ,'aws' AS provider
        ,a.account_name
        ,a.circle_id
        ,a.aws_product_name AS product_name
        ,a.resource_id AS resource_id
        ,a.tag_name AS tag_name
        ,'aws_resource_id' AS resource_type
        ,SUM(a.unblended_cost + a.reservation_cost) AS total_cost
    FROM
        {{ ref('aws_costs_day_usage') }} a
    LEFT JOIN data_platform d ON
        ('aws/' || coalesce(a.account_id, '') || '/' || coalesce(a.region, '') || '/' || coalesce(a.resource_id, '') || '/' || coalesce(a.usage_type, '') || '/' || coalesce(a.pricing_term, '')) = d.resource_id
        AND DATE(a.start_date) = DATE(d.start_date)
    WHERE d.resource_id IS NULL
    GROUP BY
        8,7,6,5,4,3,2,1
),

aws_untagged AS (
    SELECT
        CAST(start_date AS DATE) AS start_date
        ,'aws' AS provider
        ,account_name
        ,circle_id
        ,aws_product_name AS product_name
        ,resource_id AS resource_id
        ,tag_name AS tag_name
        ,'aws_resource_id' AS resource_type
        ,SUM(unblended_cost + reservation_cost) AS total_cost
    FROM
        {{ ref('aws_costs_day_untagged') }}
    GROUP BY
        8,7,6,5,4,3,2,1
),

mongodb AS (
    SELECT
        CAST(start_date AS DATE) AS start_date
        ,'mongodb' AS provider
        ,'mongodb' AS account_name
        ,circle_id
        ,sku AS product_name
        ,cluster_name AS resource_id
        ,cluster_name AS tag_name
        ,'mongodb_atlas_cluster_name' AS resource_type
        ,SUM(cost) AS total_cost
    FROM
        {{ ref('mongodb_atlas_costs_day_usage') }}
    GROUP BY
        8,7,6,5,4,3,2,1
),

datadog AS (
    SELECT
        CAST(month AS DATE) AS start_date
        ,'datadog' AS provider
        ,'datadog' AS account_name
        ,circle_id
        ,product_name
        ,service AS resource_id
        ,service AS tag_name
        ,'datadog_service' AS resource_type
        ,SUM(cost) AS total_cost
    FROM
        {{ ref('datadog_costs_month') }}
    GROUP BY
        8,7,6,5,4,3,2,1
),

final AS (
    SELECT * FROM eks
    UNION ALL
    SELECT * FROM aws_costs
    UNION ALL
    SELECT * FROM aws_untagged
    UNION ALL
    SELECT * FROM mongodb
    UNION ALL
    SELECT * FROM datadog
    UNION ALL
    SELECT * FROM data_platform
)

SELECT
    start_date
    ,provider
    ,account_name
    ,circle_id
    ,product_name
    ,resource_id
    ,tag_name
    ,resource_type
    ,total_cost
FROM final
