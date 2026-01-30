WITH
tag_product_cluster_name(cluster_name, environment, tag_product) AS (
    VALUES
         ('will-prod-shared',               'production', 'EKS - Cluster Shared')
        ,('will-dev-shared',                'development','EKS - Cluster Shared')
        ,('will-prod-foundation-platforms', 'production', 'EKS - Cluster Foundation Platforms')
        ,('will-dev-foundation-platforms',  'development','EKS - Cluster Foundation Platforms')
        ,('will-dev-test',                  'development','EKS - Cluster Test')
        ,('analytics-prod-credit-decision', 'production', 'EKS - Credit Decision')
        ,('analytics-dev-credit-decision',  'development','EKS - Credit Decision')
),

tag_name_cluster_name(tag_name, cluster_name) AS (
    VALUES
         ('will-prod-shared',               'will-prod-shared')
        ,('will-dev-shared',                'will-dev-shared')
        ,('will-prod-foundation-platforms', 'will-prod-foundation-platforms')
        ,('will-dev-foundation-platforms',  'will-dev-foundation-platforms')
        ,('analytics-prod-credit-decision', 'analytics-prod-credit-decision')
        ,('analytics-dev-credit-decision',  'analytics-dev-credit-decision')
        ,('will-dev-test',                  'will-dev-test')
        ,('eks-will-prod',                  'eks-will-prod-01')
        ,('eks-will-dev',                   'eks-will-dev-02')
),

aws AS (
SELECT
    DATE_TRUNC('month', CAST(start_date as TIMESTAMP)) as start_date
    ,a.aws_product_name
    ,COALESCE(
        tncn.cluster_name
        ,tpcn.cluster_name
        ,'Not Tagged'
    ) AS cluster_name
    ,'Usage' AS cost_type
    ,sum(a.reservation_cost + a.unblended_cost) AS cost
FROM
    {{ ref('aws_costs_day_usage') }} a
LEFT JOIN
    tag_name_cluster_name tncn ON a.tag_name LIKE '%' || tncn.tag_name || '%'
LEFT JOIN
    tag_product_cluster_name tpcn ON a.tag_product = tpcn.tag_product AND a.environment = tpcn.environment
WHERE
    a.tag_product LIKE 'EKS - %'
    OR a.tag_name = 'eks-will-prod-01'
    OR a.tag_name = 'eks-will-dev-02'
GROUP BY
    4,3,2,1
ORDER BY
    sum(a.reservation_cost + a.unblended_cost) DESC
),

eks AS (
SELECT
    DATE_TRUNC('month', CAST(start_date as TIMESTAMP)) as start_date
    ,'Amazon Elastic Compute Cloud' AS aws_product_name
    ,cluster_name
    ,'Discount' AS cost_type
    ,SUM(split_cost + unused_cost) AS cost
FROM
    {{ ref('aws_eks_costs_day_usage') }}
WHERE
    split_cost < 0 OR unused_cost < 0
GROUP BY
    4,3,2,1
ORDER BY
    SUM(split_cost + unused_cost) DESC
)

SELECT * FROM aws
UNION ALL
SELECT * FROM eks
