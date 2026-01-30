SELECT
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    ,DATE_TRUNC('day', CAST(end_date AS TIMESTAMP)) AS end_date
    ,cluster_name
    ,sku
    ,circle_id
    ,tag_policy_compliant
    ,environment
    ,CAST(SUM(total_price_cents) AS decimal(10,2))/100 AS cost
FROM
    {{ ref('mongodb_atlas_costs_usage') }}
WHERE
    start_date < DATE_TRUNC('day', current_date)
GROUP BY
    7,6,5,4,3,2,1
ORDER BY
    1
