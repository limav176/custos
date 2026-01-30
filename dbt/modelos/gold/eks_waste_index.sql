WITH provisioned_costs AS (
    SELECT
        date_trunc('day', CAST(start_date AS TIMESTAMP)) AS start_date,
        namespace,
        cluster_name,
        environment,
        circle_id,

        CAST(SUM(
            CASE
                WHEN usage_type LIKE '%EKS-EC2-vCPU-Hours' THEN (split_cost + unused_cost)
                ELSE 0
            END
        ) AS DECIMAL(10,2)) AS cpu_cost,

        CAST(SUM(
            CASE
                WHEN usage_type LIKE '%EKS-EC2-GB-Hours' THEN (split_cost + unused_cost)
                ELSE 0
            END
        ) AS DECIMAL(10,2)) AS memory_cost,

        CAST(SUM(split_cost + unused_cost) AS DECIMAL(10,2)) AS total_cost,

        CAST(SUM(
            CASE
                WHEN usage_type LIKE '%EKS-EC2-vCPU-Hours' OR usage_type LIKE '%EKS-EC2-GB-Hours' THEN unused_cost
                ELSE 0
            END
        ) AS DECIMAL(10,2)) AS waste_cost,

        CAST(SUM(
            CASE
                WHEN usage_type LIKE '%EKS-EC2-vCPU-Hours' THEN unused_cost
                ELSE 0
            END
        ) AS DECIMAL(10,2)) AS cpu_waste_cost,

        CAST(SUM(
            CASE
                WHEN usage_type LIKE '%EKS-EC2-GB-Hours' THEN unused_cost
                ELSE 0
            END
        ) AS DECIMAL(10,2)) AS memory_waste_cost
    FROM
        {{ ref('aws_eks_costs_day_usage') }}
    WHERE cluster_name NOT IN ('eks-will-prod-01', 'eks-will-dev-02')
    GROUP BY
        date_trunc('day', CAST(start_date AS TIMESTAMP)),
        namespace,
        cluster_name,
        environment,
        circle_id
)

SELECT
    start_date,
    namespace,
    cluster_name,
    environment,
    circle_id,

    'CPU' AS usage_category,
    total_cost,
    cpu_cost,
    NULL AS memory_cost,
    waste_cost,
    cpu_waste_cost,
    NULL AS memory_waste_cost,

    CASE
        WHEN cpu_cost <= 0 THEN 0
        ELSE (cpu_waste_cost / NULLIF(cpu_cost, 0))
    END AS cpu_waste_index,

    NULL AS memory_waste_index,
    NULL AS total_waste_index,

    CASE
        WHEN (cpu_waste_cost / NULLIF(cpu_cost, 0)) >= 0.25 THEN 'Reduzir CPU request'
        ELSE 'No Reduction Opportunities'
    END AS recommendation
FROM
    provisioned_costs
WHERE cpu_cost > 0

UNION ALL

SELECT
    start_date,
    namespace,
    cluster_name,
    environment,
    circle_id,

    'Memory' AS usage_category,
    total_cost,
    NULL AS cpu_cost,
    memory_cost,
    waste_cost,
    NULL AS cpu_waste_cost,
    memory_waste_cost,

    NULL AS cpu_waste_index,

    CASE
        WHEN memory_cost <= 0 THEN 0
        ELSE (memory_waste_cost / NULLIF(memory_cost, 0))
    END AS memory_waste_index,

    NULL AS total_waste_index,

    CASE
        WHEN (memory_waste_cost / NULLIF(memory_cost, 0)) >= 0.25 THEN 'Reduzir MEMORY request'
        ELSE 'No Reduction Opportunities'
    END AS recommendation
FROM
    provisioned_costs
WHERE memory_cost > 0

UNION ALL

SELECT
    start_date,
    namespace,
    cluster_name,
    environment,
    circle_id,

    'Total' AS usage_category,
    total_cost,
    NULL AS cpu_cost,
    NULL AS memory_cost,
    waste_cost,
    NULL AS cpu_waste_cost,
    NULL AS memory_waste_cost,

    NULL AS cpu_waste_index,
    NULL AS memory_waste_index,

    CASE
        WHEN cpu_waste_cost > 0 AND memory_waste_cost > 0 THEN (cpu_waste_cost / NULLIF(cpu_cost, 0) + memory_waste_cost / NULLIF(memory_cost, 0)) / 2
        WHEN cpu_waste_cost > 0 THEN (cpu_waste_cost / NULLIF(cpu_cost, 0))
        WHEN memory_waste_cost > 0 THEN (memory_waste_cost / NULLIF(memory_cost, 0))
        ELSE 0
    END AS total_waste_index,

    CASE
        WHEN (cpu_waste_cost / NULLIF(cpu_cost, 0)) >= 0.25 THEN 'Reduzir CPU request'
        WHEN (memory_waste_cost / NULLIF(memory_cost, 0)) >= 0.25 THEN 'Reduzir MEMORY request'
        ELSE 'No Reduction Opportunities'
    END AS recommendation
FROM
    provisioned_costs
WHERE cpu_cost > 0 OR memory_cost > 0

ORDER BY
    start_date,
    namespace,
    cluster_name,
    usage_category
