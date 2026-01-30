WITH
rivery AS (
    SELECT * FROM {{ ref('aux_rds_reservations') }}
),

aws AS (
SELECT
    aws_product_name
    ,start_date
    ,reservation_start_time
    ,reservation_end_time
    ,reservation_arn
    ,reservation_id
    ,region
    ,reservations
    ,reservation_hours
    ,instance_commited
    ,usage_type
    ,contract_length
    ,pricing_term
    ,offering_class
    ,contract_purchase
    ,unblended_cost
FROM
    {{ ref('stg_aws_cost_usage_report') }}
WHERE
    reservation_start_time IS NOT NULL
    AND usage_type LIKE '%HeavyUsage:db.%'
    AND account_id = '887592687927'
    AND YEAR(reservation_start_time) >= 2025
    AND aws_product_name = 'Amazon Relational Database Service'
GROUP BY 16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
ORDER BY 2 ASC
)

SELECT
    a.start_date
    ,r.reservation_start_time
    ,r.reservation_end_time
    ,a.reservation_id
    ,r.engine
    ,r.multi_az
    ,a.region
    ,r.circle_id
    ,r.reservations
    ,a.instance_commited
    ,a.contract_length
    ,a.offering_class
    ,a.contract_purchase
    ,SUM(a.reservation_hours*r.reservations) AS reservation_hours
    ,SUM((a.unblended_cost/a.reservations)*r.reservations) AS reservation_cost
FROM
    aws a
LEFT JOIN
    rivery r ON r.reservation_id = a.reservation_id
WHERE
    a.start_date >= r.reservation_start_time
    AND a.start_date <= r.reservation_end_time
GROUP BY 13,12,11,10,9,8,7,6,5,4,3,2,1
ORDER BY 1 ASC
