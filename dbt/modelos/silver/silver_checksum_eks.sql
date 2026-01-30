WITH silver_pre_eks_usage AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        SUM(split_cost + unused_cost) AS cost
    FROM
        {{ ref('silver_aws_eks_cost_usage_report') }}
    WHERE
        COALESCE(billing_origin, '') <> 'AWS Marketplace'
        AND COALESCE(aws_product_name, '') <> 'OCBAWS Brazil 2P'
    GROUP BY 1
),

silver_eks_usage AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        SUM(split_cost + unused_cost) AS cost
    FROM
        {{ ref('aws_eks_costs_usage') }}
    WHERE namespace <> 'empty'
    GROUP BY 1
)

SELECT
    pre.start_date,
    ROUND(pre.cost, 2) AS "silver_pre_eks_usage_cost",
    ROUND(s.cost, 2) AS "silver_eks_usage_cost"
FROM
    silver_pre_eks_usage pre
LEFT JOIN
    silver_eks_usage s ON pre.start_date = s.start_date
