-- CUR
WITH cur AS (
    SELECT
        DATE_TRUNC('month', CAST(line_item_usage_start_date AS TIMESTAMP)) AS start_date,
        'aws_cost_usage_report' as table_name,
        resource_tags_user_circle_id as circle_id,
        product_product_name as aws_product_name,
        SUM(line_item_unblended_cost + reservation_effective_cost) AS cost
    FROM
        {{ source('custos_cloud_bronze', 'aws_cost_usage_report') }}
    WHERE
        line_item_operation <> 'EKSPod-EC2'
        AND line_item_line_item_type <> 'DiscountedUsage'
    GROUP BY 1,2,3,4
)

SELECT 
    start_date
    ,table_name
    ,CASE 
        WHEN LENGTH(circle_id) <> 24 THEN NULL 
        ELSE circle_id 
    END AS circle_id
    ,aws_product_name
    ,cost
FROM cur