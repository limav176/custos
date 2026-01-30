with
month as (
    SELECT
        start_date
        ,COALESCE(nullif(circle_id, '') , 'empty') as circle_id
        ,COALESCE(nullif(cluster_name, '') , 'empty') as cluster_name
        ,COALESCE(nullif(sku, '') , 'empty') as sku
        ,COALESCE(SUM(cost), 0) as cost
    FROM
        {{ ref('mongodb_atlas_costs_month_usage') }}
    GROUP BY
        4,3,2,1

    UNION ALL

    SELECT
        start_date
        ,COALESCE(nullif(circle_id, '') , 'empty') as circle_id
        ,''
        ,COALESCE(nullif(sku, '') , 'empty') as sku
        ,COALESCE(SUM(cost), 0) as cost
    FROM
        {{ ref('mongodb_atlas_costs_month_support') }}
    GROUP BY
        4,3,2,1
),

source as (
SELECT
     COALESCE(current_month.circle_id, prev_month.circle_id, '999999999999999999999999') as circle_id
    ,COALESCE(current_month.cluster_name, prev_month.cluster_name) as cluster_name
    ,COALESCE(current_month.sku, prev_month.sku) as sku
    ,COALESCE(current_month.start_date, prev_month.start_date + interval '1' month) as month
    ,COALESCE(current_month.cost, 0)  as cost
    ,COALESCE(prev_month.start_date, current_month.start_date - interval '1' month)  as previous_month
    ,COALESCE(prev_month.cost, 0) as previous_cost
FROM
    month current_month
FULL OUTER JOIN
    month prev_month
ON
    current_month.circle_id = prev_month.circle_id
    AND current_month.cluster_name = prev_month.cluster_name
    AND current_month.sku = prev_month.sku
    AND current_month.start_date - interval '1' month = prev_month.start_date
ORDER BY
    4,3,2,1 DESC
)

SELECT * FROM source
