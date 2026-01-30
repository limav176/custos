WITH silver_checksum AS (
    SELECT
        start_date,
        silver_pre_eks_usage_cost,
        silver_eks_usage_cost
    FROM
       {{ ref('silver_checksum_eks') }}
),

gold_tables AS (
    SELECT start_date, cost FROM {{ ref('aws_eks_costs_month_cluster') }}
),

gold_month AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        SUM(cost) AS cost
    FROM
        gold_tables
    GROUP BY 1
),

gold_tables_day AS (
    SELECT start_date, cpu_cost, memory_cost, split_cost, unused_cost FROM {{ ref('aws_eks_costs_day_usage') }}
),

gold_day AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        SUM(cpu_cost + memory_cost + split_cost + unused_cost) AS cost
    FROM
        gold_tables_day
    GROUP BY 1
),

day_tables AS (
    SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date,
        namespace,
        cluster_name,
        environment,
        account_id,
        account_name,
        region,
        circle_id,
        tag_policy_compliant,
        circle_type,
        usage_type,
        SUM(CASE WHEN usage_type LIKE '%EKS-EC2-vCPU-Hours' THEN split_cost + unused_cost ELSE 0.0 END) AS cpu_cost,
        SUM(CASE WHEN usage_type LIKE '%EKS-EC2-GB-Hours' THEN split_cost + unused_cost ELSE 0.0 END) AS memory_cost,
        SUM(split_cost) AS split_cost,
        SUM(unused_cost) AS unused_cost
    FROM
        {{ ref('aws_eks_costs_usage') }}
    WHERE
        start_date < DATE_TRUNC('day', current_date)
    GROUP BY
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)),
        namespace,
        cluster_name,
        environment,
        account_id,
        account_name,
        region,
        circle_id,
        tag_policy_compliant,
        circle_type,
        usage_type
),

gold_cost AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        SUM(cpu_cost + memory_cost + split_cost + unused_cost) AS cost
    FROM
        day_tables
    GROUP BY 1
)

SELECT
    s.start_date,
    ROUND(s.silver_pre_eks_usage_cost, 2) AS "silver_pre_eks_usage_cost",
    ROUND(s.silver_eks_usage_cost, 2) AS "silver_eks_usage_cost",
    ROUND(gold_cost.cost, 2) AS "gold_cost"
FROM
    silver_checksum s
LEFT JOIN
    gold_cost ON s.start_date = gold_cost.start_date
