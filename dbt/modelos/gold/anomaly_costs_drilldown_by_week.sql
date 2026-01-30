WITH
current_week AS (
    SELECT
        CASE
            WHEN EXTRACT(day FROM current_date) BETWEEN 1 AND 7 THEN 1
            WHEN EXTRACT(day FROM current_date) BETWEEN 8 AND 14 THEN 2
            WHEN EXTRACT(day FROM current_date) BETWEEN 15 AND 21 THEN 3
            ELSE 4
        END AS current_week_number
        ,DATE_TRUNC('month', current_date) AS current_month
),
weekly_cost AS (
    SELECT
        DATE_TRUNC('month', start_date) AS month
        ,CASE
            WHEN EXTRACT(day FROM start_date) BETWEEN 1 AND 7 THEN 1
            WHEN EXTRACT(day FROM start_date) BETWEEN 8 AND 14 THEN 2
            WHEN EXTRACT(day FROM start_date) BETWEEN 15 AND 21 THEN 3
            ELSE 4
        END AS week
        ,provider
        ,circle_id
        ,product_name
        ,COALESCE(NULLIF(tag_name,'Not Tagged'), resource_id) as resource_id
        ,SUM(total_cost) AS weekly_cost
    FROM
        {{ ref('all_product_costs_by_day') }}
        --"custos_cloud_gold"."all_product_costs_by_day"
    WHERE
        start_date < CURRENT_DATE
        AND start_date >= DATE_TRUNC('month', DATE_ADD('month', -3, CURRENT_DATE))
        AND product_name NOT IN ('shared')
        AND provider NOT IN ('datadog')
        AND COALESCE(resource_id, '') <> 'Difference - Reservations'
        AND NOT (
            DATE_TRUNC('month', start_date) = (SELECT current_month FROM current_week)
            AND
            CASE
                WHEN EXTRACT(day FROM start_date) BETWEEN 1 AND 7 THEN 1
                WHEN EXTRACT(day FROM start_date) BETWEEN 8 AND 14 THEN 2
                WHEN EXTRACT(day FROM start_date) BETWEEN 15 AND 21 THEN 3
                ELSE 4
            END = (SELECT current_week_number FROM current_week)
        )
    GROUP BY
        6,5,4,3,2,1
),
previous_month_weekly_cost AS (
    SELECT
        month
        ,week
        ,provider
        ,circle_id
        ,product_name
        ,resource_id
        ,weekly_cost
        ,COALESCE(LAG(weekly_cost, 1) OVER (
            PARTITION BY week, provider, circle_id, product_name, resource_id
            ORDER BY month
        ),0) AS previous_month_weekly_cost
    FROM
        weekly_cost
),
latest_weeks AS (
    SELECT
        week
        ,MAX(month) AS latest_month
    FROM
        previous_month_weekly_cost
    GROUP BY
        week
)
SELECT
    pmwc.month
    ,pmwc.week
    ,pmwc.provider
    ,pmwc.circle_id
    ,pmwc.product_name
    ,pmwc.resource_id
    ,pmwc.weekly_cost
    ,pmwc.previous_month_weekly_cost
    ,pmwc.weekly_cost - pmwc.previous_month_weekly_cost AS cost_variation
FROM
    previous_month_weekly_cost pmwc
JOIN
    latest_weeks lw
ON
    pmwc.week = lw.week
    AND pmwc.month = lw.latest_month
WHERE
    pmwc.previous_month_weekly_cost IS NOT NULL
    AND pmwc.weekly_cost > pmwc.previous_month_weekly_cost
    AND pmwc.weekly_cost - pmwc.previous_month_weekly_cost > 50
    AND pmwc.weekly_cost > 50
    AND ABS(((pmwc.weekly_cost - pmwc.previous_month_weekly_cost) / pmwc.previous_month_weekly_cost)) > 0.1
