WITH reservations AS (
SELECT 
    DATE_TRUNC('month', start_date) AS month,
    region, 
    circle_id,
    engine,
    instance_commited,
    TRUE AS eligible_commit,
    SUM(reservation_hours) AS reservation_hours,
    SUM(reservations) AS reservations,
    SUM(reservation_cost) AS reservation_cost
FROM
   {{ ref('aws_rds_reservations') }}
GROUP BY 6,5,4,3,2,1
),

cost_usage AS (
SELECT
    start_date
    ,region
    ,circle_id
    ,resource_id
    ,engine
    ,instance_size
    ,instance_commited
    ,multiply_commited
    ,eligible_commit
    ,SUM(total_cost) AS total_cost
    ,SUM(opportunity_savings) AS opportunity_savings
FROM
   {{ ref('aws_rds_costs_by_sdk') }}
WHERE
    usage_type = 'Usage'
    AND instance_size <> 'db.serverless'
    AND engine <> 'oracle-ee'
GROUP BY 9,8,7,6,5,4,3,2,1
),

active_days AS (
    SELECT
        DATE(start_date) AS start_date,
        resource_id
    FROM cost_usage
    GROUP BY 2,1
    HAVING COUNT(*) = 24
),

activity_in_month AS (
    SELECT
        DATE_TRUNC('month', start_date) AS month,
        resource_id,
        COUNT(start_date) AS days
    FROM active_days
    GROUP BY 2,1
),

month_data_completeness AS (
    SELECT
        DATE_TRUNC('month', DATE(start_date)) AS month,
        MAX(EXTRACT(DAY FROM DATE(start_date))) AS days
    FROM cost_usage
    GROUP BY 1
),

activity_prev_month AS (
     SELECT
        month,
        resource_id,
        COUNT(start_date) AS days
    FROM (
        SELECT
            DATE_TRUNC('month', DATE_ADD('month', 1, start_date)) as month,
            resource_id,
            start_date
        FROM active_days ad
        WHERE
         ad.start_date >= DATE_ADD('day', -15, DATE_TRUNC('month', DATE_ADD('month', 1, ad.start_date)))
    ) AS relevant_prev_month_activity
    GROUP BY 2,1
),

monthly_active AS (
SELECT
    COALESCE(cur.month, prev.month) AS month,
    COALESCE(cur.resource_id, prev.resource_id) AS resource_id,
    CASE
        WHEN mdc.days < 15 THEN
            (COALESCE(prev.days, 0) >= 15)
        ELSE
            (COALESCE(cur.days, 0) >= 15)
    END AS active
FROM
    activity_in_month cur
FULL OUTER JOIN
    activity_prev_month prev
    ON cur.month = prev.month
    AND cur.resource_id = prev.resource_id
LEFT JOIN
    month_data_completeness mdc
    ON mdc.month = COALESCE(cur.month, prev.month)
),

resource_usage AS (
SELECT 
    DATE_TRUNC('month', cu.start_date) AS month,
    cu.region, 
    cu.circle_id,
    cu.resource_id,
    cu.engine,
    cu.eligible_commit,
    COALESCE(cu.instance_commited, '404 - ' || cu.instance_size) AS instance_commited, 
    SUM(cu.multiply_commited) AS instance_hours,
    CASE 
        WHEN count(*) > 0 THEN (SUM(cu.multiply_commited) * 1.0 / count(*))
        ELSE 0.0 
    END AS instances,
    SUM(cu.total_cost) AS ondemand_cost,
    SUM(cu.opportunity_savings) AS opportunity_savings
FROM
   cost_usage cu
GROUP BY 
    7,6,5,4,3,2,1
),

instances AS (
SELECT 
    ru.month,
    ru.region,
    ru.engine,
    ru.circle_id,
    ru.instance_commited,
    ru.eligible_commit,
    SUM(ru.instance_hours) AS instance_hours,
    SUM(ru.instances) AS instances,
    SUM(ru.ondemand_cost) AS ondemand_cost,
    SUM(CASE WHEN ma.active = TRUE THEN ru.opportunity_savings ELSE 0.0 END) AS opportunity_savings,
    SUM(CASE WHEN ma.active = FALSE THEN ru.instances ELSE 0 END) AS instances_inactives,
    SUM(CASE WHEN ma.active = TRUE THEN ru.instances ELSE 0 END) AS instances_actives
FROM
    resource_usage ru
LEFT JOIN 
    monthly_active ma ON ma.month = ru.month AND ma.resource_id = ru.resource_id
GROUP BY 6,5,4,3,2,1
),

cost AS (
SELECT
    COALESCE(i.month, r.month) AS month,
    COALESCE(i.region, r.region) AS region,
    COALESCE(i.circle_id, r.circle_id) AS circle_id,
    COALESCE(i.engine, r.engine) AS engine,
    COALESCE(i.instance_commited, r.instance_commited) AS instance_commited,
    COALESCE(i.eligible_commit, r.eligible_commit) AS eligible_commit,
    COALESCE(i.instances, 0) AS instances,
    COALESCE(i.instances_inactives, 0) AS instances_inactives,
    COALESCE(i.instances_actives, 0) AS instances_actives,
    COALESCE(r.reservations, 0) AS reservations,
    COALESCE(i.instance_hours, 0.0) AS instance_hours,
    COALESCE(r.reservation_hours, 0.0) AS reservation_hours,
    CASE
        WHEN COALESCE(i.instance_hours, 0.0) < COALESCE(r.reservation_hours, 0.0) THEN 0.0
        ELSE COALESCE(i.instance_hours, 0.0) - COALESCE(r.reservation_hours, 0.0)
    END AS ondemand_hours,
    COALESCE(r.reservation_cost, 0.0) AS reservation_cost,
    CASE
        WHEN COALESCE(i.instance_hours, 0.0) = 0 THEN 0.0
        WHEN COALESCE(i.instance_hours, 0.0) < COALESCE(r.reservation_hours, 0.0) THEN 0.0
        ELSE COALESCE( (COALESCE(i.ondemand_cost, 0.0) / NULLIF(COALESCE(i.instance_hours, 0.0), 0.0)) * (COALESCE(i.instance_hours, 0.0) - COALESCE(r.reservation_hours, 0.0)) , 0.0)
    END AS ondemand_cost,
    COALESCE( ( (COALESCE(i.ondemand_cost,0.0) / NULLIF(COALESCE(i.instance_hours,0.0),0.0)) * COALESCE(r.reservation_hours,0.0) ), 0.0) - COALESCE(r.reservation_cost,0.0)
    AS savings,
    CASE
        WHEN COALESCE(i.instance_hours, 0.0) = 0 THEN 0.0
        WHEN COALESCE(i.instance_hours, 0.0) < COALESCE(r.reservation_hours, 0.0) THEN 0.0
        ELSE COALESCE( (COALESCE(i.opportunity_savings, 0.0) / NULLIF(COALESCE(i.instance_hours, 0.0),0.0)) * (COALESCE(i.instance_hours, 0.0) - COALESCE(r.reservation_hours, 0.0)) , 0.0)
    END AS opportunity_savings
FROM
    instances i
FULL OUTER JOIN
    reservations r ON i.month = r.month
                AND i.region = r.region
                AND i.circle_id = r.circle_id
                AND i.engine = r.engine
                AND i.instance_commited = r.instance_commited
                AND i.eligible_commit = r.eligible_commit
),

coverage AS (
    SELECT
        month,
        engine,
        region,
        instance_commited,
        SUM(instances_actives) AS total_instances,
        SUM(COALESCE(reservations, 0)) AS total_reservations
    FROM cost
    GROUP BY 4,3,2,1
),

final AS (
SELECT 
    cost.month,
    cost.region,
    circle_id,
    cost.engine,
    cost.instance_commited,
    eligible_commit,
    instances,
    instances_inactives,
    instances_actives,
    reservations,
    instance_hours,
    reservation_hours,
    ondemand_hours,
    reservation_cost,
    ondemand_cost,
    savings,
    opportunity_savings,
    CASE 
        WHEN cost.engine NOT IN ('aurora-postgresql','aurora-mysql','mysql','postgresql')
            THEN 'Contact the observability team for a reservation study.'
        WHEN eligible_commit = FALSE 
            AND cost.region <> 'sa-east-1' 
            THEN 'We are exploring an alternative with Neon.tech to Development. More details to come.'
        WHEN eligible_commit = FALSE
            AND cost.engine NOT LIKE '%aurora%' 
            AND cost.instance_commited LIKE 'db.t3%' 
            THEN 'Change the engine to Aurora and set the instance shape to db.t4g.'
        WHEN eligible_commit = FALSE
            AND cost.engine NOT LIKE '%aurora%' 
            AND cost.instance_commited NOT IN ('db.r6g.large', 'db.r7g.large', 'db.t4g.medium') 
            THEN 'Change the engine to Aurora and set the instance shape to db.r6g or db.r7g.'
        WHEN eligible_commit = FALSE
            AND cost.instance_commited LIKE 'db.t3%' 
            THEN 'Change the instance shape to db.t4g.'
        WHEN eligible_commit = FALSE
            AND cost.instance_commited NOT IN ('db.r6g.large', 'db.r7g.large', 'db.t4g.medium') 
            THEN 'Change the instance shape to db.r6g or db.r7g.'
        WHEN eligible_commit = TRUE
            AND CAST(reservations AS DOUBLE) / CAST(instances_actives AS DOUBLE) = 1.0
            THEN 'No recommendation at this time. All instances actives are reserved.'
        WHEN eligible_commit = TRUE
            AND CAST(reservations AS DOUBLE) / CAST(instances_actives AS DOUBLE) > 1.0
            THEN 'Contact the Observability team to transfer the reservation to another circle.'
        WHEN eligible_commit = TRUE
            AND CAST(total_reservations AS DOUBLE) / CAST(NULLIF(total_instances, 0) AS DOUBLE) < 0.8
            THEN 'Contact the Observability team to reserve the instance.'
        ELSE 'No recommendation at this time. Reservations exceed AWS guidelines.'
    END AS recommendation,
    SUM(reservation_cost + ondemand_cost) AS total_cost
FROM
    cost cost
LEFT JOIN
    coverage coverage ON cost.month = coverage.month 
                    AND cost.engine = coverage.engine
                    AND cost.region = coverage.region
                    AND cost.instance_commited = coverage.instance_commited
GROUP BY 18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
)

SELECT * FROM final 