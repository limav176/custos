with
month as (
    SELECT
        start_date
        ,billing_origin
        ,aws_product_name
        ,circle_id
        ,'usage' as type
        ,SUM(cost) AS cost
    FROM
        {{ ref('aws_costs_month_usage') }}
    GROUP BY
        5,4,3,2,1

    UNION ALL

    SELECT
        start_date
        ,billing_origin
        ,aws_product_name
        ,circle_id
        ,'untagged' as type
        ,SUM(cost) AS cost
    FROM
        {{ ref('aws_costs_month_untagged') }}
    GROUP BY
        5,4,3,2,1

    UNION ALL

    SELECT
        start_date
        ,billing_origin
        ,aws_product_name
        ,circle_id
        ,'support' as type
        ,SUM(cost) AS cost
    FROM
        {{ ref('aws_costs_month_support') }}
    GROUP BY
        5,4,3,2,1

    UNION ALL

    SELECT
        start_date
        ,billing_origin
        ,'AWS shared costs' as aws_product_name
        ,circle_id
        ,'shared' as type
        ,SUM(cost) AS cost
    FROM
        {{ ref('aws_costs_month_shared') }}
    GROUP BY
        5,4,3,2,1

    UNION ALL

    SELECT
        start_date
        ,billing_origin
        ,aws_product_name
        ,'999999999999999999999999' as circle_id
        ,'marketplace' as type
        ,SUM(cost) AS cost
    FROM
        {{ ref('aws_costs_month_marketplace') }}
    GROUP BY
        5,4,3,2,1

),

source as (
SELECT
     COALESCE(current_month.billing_origin, prev_month.billing_origin) as billing_origin
    ,COALESCE(current_month.aws_product_name, prev_month.aws_product_name) as aws_product_name
    ,COALESCE(current_month.circle_id, prev_month.circle_id, '999999999999999999999999') as circle_id
    ,COALESCE(current_month.type, prev_month.type) as type
    ,COALESCE(current_month.start_date, prev_month.start_date + interval '1' month) as month
    ,COALESCE(current_month.cost, 0)  as cost
    ,COALESCE(prev_month.start_date, current_month.start_date - interval '1' month)  as previous_month
    ,COALESCE(prev_month.cost, 0) as previous_cost
FROM
    month current_month
FULL OUTER JOIN
    month prev_month
ON
    current_month.billing_origin = prev_month.billing_origin
    AND current_month.aws_product_name = prev_month.aws_product_name
    AND current_month.circle_id = prev_month.circle_id
    AND current_month.type = prev_month.type
    AND current_month.start_date - interval '1' month = prev_month.start_date
ORDER BY
    2,1 DESC
)

SELECT * FROM source
