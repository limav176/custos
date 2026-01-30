WITH
aws AS (
    SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
        ,resource_id
        ,description
        ,billing_origin
        ,aws_product_name
        ,'' AS namespace
        ,'' AS cluster_name
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
        ,CAST(SUM(reservation_cost+unblended_cost) AS decimal(10,2)) AS cost
        ,0.0 AS nr_datascannedinbytes
    FROM
        {{ ref('aws_costs_usage') }}
    WHERE
        start_date > (current_date - interval '90' day)
        AND aws_product_name <> 'Amazon Athena'
    GROUP BY
        21,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),

eks AS (
    SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
        ,instance_id as resource_id
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
        ,CAST(SUM(split_cost + unused_cost) AS decimal(10,2)) AS cost
        ,0.0 AS nr_datascannedinbytes
    FROM
        {{ ref('aws_eks_costs_usage') }}
    WHERE
        start_date > (current_date - interval '90' day)
    GROUP BY
        21,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
    ORDER BY
        1
),

athena AS (
    SELECT
        DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
        ,workgroup as resource_id
        ,CONCAT(
            CASE 
                WHEN workgroup='primary' THEN 'Queries adhoc Lake 1.0 - '
                WHEN workgroup='dbt_workgroup' THEN 'Processamento Lake 1.0 - '
                WHEN workgroup='looker_workgroup' THEN 'Looker Lake 1.0 - '
                ELSE 'Lake 2.0 - '
            END,
            CASE
                WHEN nr_datascannedinbytes >= 1099511627776 THEN 
                    CONCAT(CAST(CAST(nr_datascannedinbytes AS DECIMAL(38,2)) / 1099511627776 AS VARCHAR), ' TB')
                WHEN nr_datascannedinbytes >= 1073741824 THEN 
                    CONCAT(CAST(CAST(nr_datascannedinbytes AS DECIMAL(38,2)) / 1073741824 AS VARCHAR), ' GB')
                WHEN nr_datascannedinbytes >= 1048576 THEN 
                    CONCAT(CAST(CAST(nr_datascannedinbytes AS DECIMAL(38,2)) / 1048576 AS VARCHAR), ' MB')
                ELSE 
                    CONCAT(CAST(CAST(nr_datascannedinbytes AS DECIMAL(38,2)) / 1024 AS VARCHAR), ' KB')
            END,
            ' scanned'
        ) AS description
        ,billing_origin
        ,aws_product_name
        ,'' AS namespace
        ,'' AS cluster_name
        ,circle_id
        ,providedby_id
        ,'' AS circle_type
        ,'' AS tag_name
        ,'' AS tag_repository
        ,'' AS tag_product
        ,'' AS tag_management
        ,true AS tag_policy_compliant
        ,environment
        ,account_id
        ,account_name
        ,region
        ,SUM(total_cost) AS cost
        ,SUM(nr_datascannedinbytes) AS nr_datascannedinbytes
    FROM
        {{ ref('aws_athena_costs_day_usage') }}
    WHERE
        start_date > (current_date - interval '90' day)
    GROUP BY
        19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
    ORDER BY
        1
)

SELECT * FROM aws
UNION ALL
SELECT * FROM eks
UNION ALL
SELECT * FROM athena