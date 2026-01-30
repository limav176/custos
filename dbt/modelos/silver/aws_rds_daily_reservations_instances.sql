WITH
-- Dados de instâncias RDS ativas por dia
instances_daily AS (
    SELECT
        CAST(start_date AS DATE) AS start_date,
        'Amazon Relational Database Service' AS aws_product_name,
        region,
        engine,
        multi_az,
        COALESCE(instance_commited, '404 - ' || instance_size) AS instance_commited,
        circle_id,
        COUNT(DISTINCT resource_id) AS instances
    FROM {{ ref('aws_rds_instances_details') }}
    WHERE status = TRUE
        AND engine NOT IN ('oracle-ee')
        AND instance_size <> 'db.serverless'
        AND start_date IS NOT NULL
        AND eligible_commit = TRUE
        AND YEAR(start_date) >= 2025
    GROUP BY 7,6,5,4,3,2,1
    ORDER BY 1 ASC
),

-- Dados de reservas RDS por dia
reservations_daily AS (
SELECT
	start_date
	,aws_product_name
	,region
	,engine
	,multi_az
	,instance_commited
	,circle_id
	,SUM(reservations) AS reservations
FROM
	(SELECT
        CAST(date_series AS DATE) AS start_date,
        'Amazon Relational Database Service' AS aws_product_name,
        region,
        engine,
        multi_az,
        instance_commited,
        circle_id,
        reservations
    FROM {{ ref('aws_rds_reservations') }}
    CROSS JOIN UNNEST(
        SEQUENCE(
            reservation_start_time, 
            reservation_end_time, 
            INTERVAL '1' DAY
        )
    ) AS t(date_series)
    WHERE start_date IS NOT NULL
    GROUP BY 8,7,6,5,4,3,2,1) 
GROUP BY 7,6,5,4,3,2,1
ORDER BY 1 ASC
),

-- Combinação de instâncias e reservas
combined_data AS (
    SELECT
        COALESCE(i.start_date, r.start_date) AS start_date,
        COALESCE(i.aws_product_name, r.aws_product_name) AS aws_product_name,
        COALESCE(i.region, r.region) AS region,
        COALESCE(i.engine, r.engine) AS engine,
        COALESCE(i.multi_az, r.multi_az) AS multi_az,
        COALESCE(i.instance_commited, r.instance_commited) AS instance_commited,
        COALESCE(i.circle_id, r.circle_id) AS circle_id,
        COALESCE(i.instances, 0) AS instances,
        COALESCE(r.reservations, 0) AS reservations
    FROM instances_daily i
    FULL OUTER JOIN reservations_daily r
        ON i.region = r.region
        AND i.start_date = r.start_date
        AND i.engine = r.engine
        AND i.multi_az = r.multi_az
        AND i.instance_commited = r.instance_commited
        AND COALESCE(i.circle_id, '999999999999999999999999') = COALESCE(r.circle_id, '999999999999999999999999')
)

SELECT
    start_date,
    aws_product_name,
    region,
    engine,
    multi_az,
    instance_commited,
    circle_id,
    instances,
    reservations,
    (instances - reservations) AS without_reservations
FROM combined_data
WHERE start_date IS NOT null
GROUP BY 10,9,8,7,6,5,4,3,2,1
ORDER BY start_date ASC, region, engine, multi_az, instance_commited, circle_id 