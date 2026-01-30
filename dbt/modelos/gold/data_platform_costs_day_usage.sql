WITH

circles AS (
    SELECT
        circle_id,
        circle_name,
        type AS circle_type
    FROM
        {{ ref('holaspirit_circles') }}
        --"custos_cloud_silver"."holaspirit_circles"
),

aws_costs AS (
    SELECT
        ('aws/' || coalesce(account_id, '') || '/' || coalesce(region, '') || '/' || coalesce(resource_id, '') || '/' || coalesce(usage_type, '') || '/' || coalesce(pricing_term, '')) AS resource_id,
        DATE(start_date) AS start_date,
        environment,
        account_id,
        account_name,
        billing_origin,
        region,
        aws_product_name,
        circle_id,
        providedby_id,
        circle_type,
        SUM(unblended_cost) + SUM(reservation_cost) AS cost
    FROM
        {{ ref('aws_costs_usage') }}
        --"custos_cloud_silver"."aws_costs_usage"
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

eks_costs AS (
    SELECT
        ('eks/' || coalesce(account_id, '') || '/' || coalesce(region, '') || '/' || coalesce(cluster_name, '') || '/' || coalesce(namespace, '')) AS resource_id,
        DATE(start_date) AS start_date,
        environment,
        account_id,
        account_name,
        billing_origin,
        region,
        aws_product_name,
        SUM(split_cost) + SUM(unused_cost) AS cost
    FROM
        {{ ref('aws_eks_costs_usage') }}
        --"custos_cloud_silver"."aws_eks_costs_usage"
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8
),

data_platform_usage AS (
    SELECT
        dt_execution,
        id_resource,
        id_specific_resource,
        product_name,
        nr_usage_metric,
        nm_usage_metric_name,
        CASE WHEN id_circulo = 'unknown' OR id_circulo IS NULL THEN '6221311e93a3d46920739acd' ELSE id_circulo END AS id_circulo,
        COALESCE(
            CAST(nr_usage_metric AS DOUBLE)
            / NULLIF(
                SUM(CAST(nr_usage_metric AS DOUBLE)) OVER (PARTITION BY dt_execution, id_resource),
                0.0
            ),
            0.0
        ) AS ratio
    FROM
        {{ source('datametrics_gold', 'plataforma_metricas_uso') }}
        --"datametrics_gold"."plataforma_metricas_uso"
),

joined AS (
    SELECT
        m.id_resource,
        m.id_specific_resource,
        m.dt_execution AS start_date,
        COALESCE(a.environment, e.environment) AS environment,
        COALESCE(a.account_id, e.account_id) AS account_id,
        COALESCE(a.account_name, e.account_name) AS account_name,
        COALESCE(a.billing_origin, e.billing_origin) AS billing_origin,
        COALESCE(a.region, e.region) AS region,
        m.product_name,
        m.id_circulo AS circle_id,
        '6408d55611798f10f203fe5b' AS providedby_id,
        c.circle_type,
        nr_usage_metric,
        nm_usage_metric_name,
        (COALESCE(a.cost, e.cost) * ratio) AS cost
    FROM data_platform_usage m
    LEFT JOIN circles c ON c.circle_id = m.id_circulo
    LEFT JOIN aws_costs a ON a.resource_id = m.id_resource
            AND a.start_date = m.dt_execution
    LEFT JOIN eks_costs e ON e.resource_id = m.id_resource
            AND e.start_date = m.dt_execution
    WHERE 1=1
        AND (
            a.resource_id IS NOT NULL
            OR e.resource_id IS NOT NULL
        )
)

SELECT
    id_resource,
    id_specific_resource,
    start_date,
    environment,
    account_id,
    account_name,
    billing_origin,
    region,
    product_name,
    circle_id,
    providedby_id,
    circle_type,
    nr_usage_metric,
    nm_usage_metric_name,
    cost
FROM
    joined
