WITH
rds AS (
SELECT
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    ,account_id
    ,account_name
    ,environment
    ,region
    ,circle_id
    ,aws_product_name
    ,instance_name AS resource_name
    ,resource_id
    ,recommendation
    ,CAST(SUM(waste_cost) AS decimal(10,2)) AS waste_cost
    ,CAST(SUM(total_cost) AS decimal(10,2)) AS total_cost
FROM
    {{ ref('aws_rds_waste_index') }}
    --"custos_cloud_silver"."aws_rds_waste_index"
GROUP BY
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP))
    ,account_id
    ,account_name
    ,environment
    ,region
    ,circle_id
    ,aws_product_name
    ,instance_name
    ,resource_id
    ,recommendation
ORDER BY start_date ASC
)

SELECT * FROM rds
