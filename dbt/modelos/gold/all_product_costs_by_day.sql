WITH final AS (
    SELECT
        CAST(start_date AS DATE) AS start_date
        ,'kubernetes' AS provider
        ,account_name
        ,circle_id
        ,'kubernetes' AS product_name
        ,namespace AS resource_id
        ,namespace AS tag_name
        ,'kubernetes_namespace' AS resource_type
        ,SUM(split_cost + unused_cost) AS total_cost
    FROM {{ ref('aws_eks_costs_day_usage') }}
    GROUP BY 8,7,6,5,4,3,2,1

    UNION ALL

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
    FROM {{ ref('aws_costs_day_usage') }}
    GROUP BY 8,7,6,5,4,3,2,1

    UNION ALL

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
    FROM {{ ref('aws_costs_day_untagged') }}
    GROUP BY 8,7,6,5,4,3,2,1

    UNION ALL

    SELECT
        CAST(start_date AS DATE) AS start_date
        ,'mongodb' AS provider
        ,'mongodb' AS account_name
        ,circle_id
        ,sku AS product_name
        ,cluster_name AS resource_id
        ,cluster_name AS tag_name
        ,'mongodb_atlas_cluster_name' as resource_type
        ,SUM(cost) AS total_cost
    FROM {{ ref('mongodb_atlas_costs_day_usage') }}
    GROUP BY 8,7,6,5,4,3,2,1

    UNION ALL

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
    FROM {{ ref('datadog_costs_month') }}
    GROUP BY 8,7,6,5,4,3,2,1
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
