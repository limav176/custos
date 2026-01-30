SELECT
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    ,namespace
    ,cluster_name
    ,instance_id
    ,environment
    ,account_id
    ,account_name
    ,region
    ,circle_id
    ,providedby_id
    ,tag_policy_compliant
    ,tag_policy_compliant_actual
    ,tag_name
    ,tag_repository
    ,tag_product
    ,tag_management
    ,circle_type
    ,usage_type
    ,CAST(SUM(
        CASE WHEN
            "usage_type"
        LIKE
            '%EKS-EC2-vCPU-Hours'
        THEN
            "split_cost" + "unused_cost"
        ELSE
            0.0
        END) AS decimal(10,2)) AS cpu_cost
    ,CAST(SUM(
        CASE WHEN
            "usage_type"
        LIKE
            '%EKS-EC2-GB-Hours'
        THEN
            "split_cost" + "unused_cost"
        ELSE
            0.0
        END) AS decimal(10,2)) AS memory_cost
    ,CAST(SUM(split_cost) AS decimal(10,2)) AS split_cost
    ,CAST(SUM(unused_cost) AS decimal(10,2)) AS unused_cost
FROM
    {{ ref('aws_eks_costs_usage') }}
WHERE start_date < DATE_TRUNC('day', current_date)
GROUP BY
    18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
ORDER BY
    1
