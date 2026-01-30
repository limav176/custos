WITH cost_usage AS (
SELECT
    start_date
    ,account_name
    ,region
    ,circle_id
    ,resource_id
    ,instance_name
    ,engine
    ,instance_size
    ,instance_commited
    ,multiply_commited
    ,eligible_commit
FROM
   {{ ref('aws_rds_costs_by_sdk') }}
WHERE
    usage_type = 'Usage'
    AND instance_size <> 'db.serverless'
    AND engine <> 'oracle-ee'
GROUP BY 11,10,9,8,7,6,5,4,3,2,1
),

waste_index AS (
SELECT
    resource_id,
    CASE WHEN recommendation IS NULL AND MAX(instance_size) NOT LIKE 'db.%g.%' THEN 0.1
        WHEN COALESCE(SUM(total_cost), 0) = 0 OR recommendation IS NULL THEN 0.0
        ELSE SUM(waste_cost) / SUM(total_cost)
    END AS waste_index,
    TRIM(BOTH ' -' FROM REPLACE(recommendation, 'Change instance size to graviton', '')) AS recommendation
FROM
    {{ ref('aws_rds_waste_index') }}
WHERE DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) > (current_date - interval '8' day)
GROUP BY resource_id, recommendation
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
    account_name,
    region,
    circle_id,
    active,
    eligible_commit,
    cu.resource_id,
    instance_name,
    engine,
    instance_size,
    COALESCE(instance_commited, '404 - ' || instance_size) AS instance_commited,
    count(*) AS instance_hours,
    count(*) * CAST(AVG(multiply_commited) AS INT) AS instance_hours_normalized,
    CAST(AVG(multiply_commited) AS INT) AS instance_amount_normalized
FROM
    cost_usage cu
LEFT JOIN
    monthly_active ma ON ma.month = DATE_TRUNC('month', cu.start_date)
                    AND ma.resource_id = cu.resource_id
GROUP BY 11,10,9,8,7,6,5,4,3,2,1
),

final AS (
SELECT
    month,
    account_name,
    region,
    circle_id,
    active,
    eligible_commit,
    ru.resource_id,
    instance_name,
    engine,
    instance_size,
    instance_commited,
    instance_hours,
    instance_hours_normalized,
    instance_amount_normalized,
    TRIM( BOTH ' -' FROM
        CASE
            WHEN engine NOT IN ('aurora-postgresql','aurora-mysql','mysql','postgresql')
                AND active = FALSE
                THEN COALESCE(wi.recommendation,'') || ' - The instance uptime is less than 15 days. Reservation is not recommended.'
            WHEN engine NOT IN ('aurora-postgresql','aurora-mysql','mysql','postgresql')
                AND active = TRUE
                THEN COALESCE(wi.recommendation,'') || ' - Contact the observability team for a reservation study.'
            WHEN eligible_commit = FALSE
                AND region <> 'sa-east-1'
                THEN  COALESCE(wi.recommendation,'') || ' - We are exploring an alternative with Neon.tech to Development.'
            WHEN eligible_commit = FALSE
                AND engine NOT LIKE '%aurora%'
                AND instance_commited LIKE 'db.t3%'
                THEN COALESCE(wi.recommendation,'') || ' - Change the engine to Aurora and set the instance shape to db.t4g.'
            WHEN eligible_commit = FALSE
                AND engine NOT LIKE '%aurora%'
                AND instance_commited NOT IN ('db.r6g.large', 'db.r7g.large', 'db.t4g.medium')
                THEN COALESCE(wi.recommendation,'') || ' - Change the engine to Aurora and set the instance shape to db.r6g or db.r7g.'
            WHEN eligible_commit = FALSE
                AND instance_commited LIKE 'db.t3%'
                THEN COALESCE(wi.recommendation,'') || ' - Change the instance shape to db.t4g.'
            WHEN eligible_commit = FALSE
                AND instance_commited NOT IN ('db.r6g.large', 'db.r7g.large', 'db.t4g.medium')
                THEN COALESCE(wi.recommendation,'') || ' - Change the instance shape to db.r6g or db.r7g.'
            WHEN eligible_commit = TRUE
                AND active = FALSE
                THEN COALESCE(wi.recommendation,'') || ' - The instance uptime is less than 15 days. Reservation is not recommended.'
            WHEN eligible_commit = FALSE
                AND region = 'sa-east-1'
                AND instance_commited IN ('db.r6g.large', 'db.r7g.large', 'db.t4g.medium')
                AND active = TRUE
                AND engine LIKE '%aurora%'
                THEN COALESCE(wi.recommendation,'') || ' - Reservations are not available for IO Optimized instances.'
            WHEN wi.recommendation IS NOT NULL
                THEN wi.recommendation
            ELSE 'No recommendation available for this instance.'
        END) AS recommendation,
    COALESCE(waste_index, 0.0) AS waste_index
FROM
    resource_usage ru
LEFT JOIN
    waste_index wi ON wi.resource_id = ru.resource_id
)

SELECT * FROM final
