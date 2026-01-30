with year_to_end as (
    select CAST(t.date AS TIMESTAMP) as start_date
    from (
        select sequence(date_trunc('year',current_date) - interval '30' day, timestamp '2025-12-31' , interval '1' day) dates
    ), unnest(dates) as t(date)
),
num_clientes(start_date, forecast_customer_count, actual_customer_count, target_month_csc) AS (
    VALUES
        (CAST('2025-01-01 00:00:00.000' AS TIMESTAMP),6757729,6748637,0.4634),
        (CAST('2025-02-01 00:00:00.000' AS TIMESTAMP),6504925,6799617,0.4691),
        (CAST('2025-03-01 00:00:00.000' AS TIMESTAMP),6670473,6908534,0.4562),
        (CAST('2025-04-01 00:00:00.000' AS TIMESTAMP),6851882,6958926,0.4393),
        (CAST('2025-05-01 00:00:00.000' AS TIMESTAMP),7049149,7072528,0.4284),
        (CAST('2025-06-01 00:00:00.000' AS TIMESTAMP),7251982,7149710,0.4205),
        (CAST('2025-07-01 00:00:00.000' AS TIMESTAMP),7469459,7220454,0.4111),
        (CAST('2025-08-01 00:00:00.000' AS TIMESTAMP),7713259,7240931,0.4041),
        (CAST('2025-09-01 00:00:00.000' AS TIMESTAMP),7958863,7269994,0.3953),
        (CAST('2025-10-01 00:00:00.000' AS TIMESTAMP),8205243,7294244,0.3945),
        (CAST('2025-11-01 00:00:00.000' AS TIMESTAMP),8419451,7305374,0.3847),
        (CAST('2025-12-01 00:00:00.000' AS TIMESTAMP),8600397,null,0.3765)
),
daily_cost as(
    SELECT
    	DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    	,sum(aws.unblended_cost + aws.reservation_cost) as cost
    FROM
    	{{ ref('silver_aws_cost_usage_report') }} aws
    WHERE
        date_trunc('day',aws.start_date) >= current_date - interval '210' day
    	AND NOT (COALESCE(aws.billing_origin, '') = 'AWS Marketplace'
    	OR COALESCE(aws.aws_product_name, '') = 'OCBAWS Brazil 2P'
        OR COALESCE(aws.description, '') = 'Contractual Credit - Credits, credit from account: 887592687927')
    GROUP BY 1
    ORDER BY 1 DESC
)

select date_trunc('day',ye.start_date) as start_date
    ,dc.cost
    ,avg(dc.cost) OVER(ORDER BY ye.start_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as cost_rolling_avg
    ,nc.forecast_customer_count as forecast_customer_count
    ,nc.actual_customer_count as actual_customer_count
    ,nc.target_month_csc as target_month_csc
from year_to_end ye
left join daily_cost dc
on ye.start_date=dc.start_date
left join num_clientes nc
on date_trunc('month',ye.start_date)=nc.start_date
order by 1
