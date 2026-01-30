WITH 

on_demand_costs AS (
  SELECT
    DATE_TRUNC('month', start_date) AS month,
    TRIM(LOWER(region)) AS region,
    TRIM(LOWER(circle_id)) AS circle_id,
    TRIM(LOWER(instance_commited)) AS instance_commited,
    SUM(total_cost) AS total_ondemand_cost
  FROM {{ ref('aws_rds_costs_by_sdk') }}
  GROUP BY 1, 2, 3, 4
),

reservations_base AS (
  SELECT
    *,
    DATE_DIFF(
      'month',
      DATE_TRUNC('month', reservation_start_time),
      DATE_TRUNC('month', reservation_end_time)
    ) + 1 AS months_count
  FROM {{ ref('aws_rds_reservations') }}
),

reservations_expanded AS (
  SELECT
    t.mth AS month,
    TRIM(LOWER(r.region)) AS region,
    TRIM(LOWER(r.circle_id)) AS circle_id,
    TRIM(LOWER(r.instance_commited)) AS instance_commited,
    (r.reservation_cost * r.reservations) / r.months_count AS monthly_cost,
    r.reservations,
    r.reservation_start_time,
    r.reservation_end_time
  FROM reservations_base r
  CROSS JOIN UNNEST(SEQUENCE(
    DATE_TRUNC('month', r.reservation_start_time),
    DATE_TRUNC('month', r.reservation_end_time),
    INTERVAL '1' MONTH
  )) AS t(mth)
),

monthly_reservation_cost AS (
  SELECT
    month,
    region,
    circle_id,
    instance_commited,
    reservation_start_time,
    reservation_end_time,
    SUM(monthly_cost) AS total_reservation_cost,
    MAX(reservations) AS reservations
  FROM reservations_expanded
  GROUP BY 1, 2, 3, 4, 5, 6
),

used_instances_per_month AS (
  SELECT
    DATE_TRUNC('month', start_date) AS month,
    TRIM(LOWER(region)) AS region,
    TRIM(LOWER(circle_id)) AS circle_id,
    TRIM(LOWER(instance_commited)) AS instance_commited,
    COUNT(DISTINCT resource_id) AS used_instances
  FROM {{ ref('aws_rds_costs_by_sdk') }}
  GROUP BY 1, 2, 3, 4
),

merged_costs AS (
  SELECT
    COALESCE(o.month, r.month) AS month,
    COALESCE(o.region, r.region) AS region,
    COALESCE(o.circle_id, r.circle_id) AS circle_id,
    COALESCE(o.instance_commited, r.instance_commited) AS instance_commited,
    COALESCE(o.total_ondemand_cost, 0) AS total_ondemand_cost,
    COALESCE(r.total_reservation_cost, 0) AS total_reservation_cost,
    r.reservation_start_time,
    r.reservation_end_time,
    r.reservations,
    u.used_instances
  FROM on_demand_costs o
  FULL OUTER JOIN monthly_reservation_cost r
    ON o.month = r.month
   AND o.region = r.region
   AND o.circle_id = r.circle_id
   AND o.instance_commited = r.instance_commited
  LEFT JOIN used_instances_per_month u
    ON COALESCE(o.month, r.month) = u.month
   AND COALESCE(o.region, r.region) = u.region
   AND COALESCE(o.circle_id, r.circle_id) = u.circle_id
   AND COALESCE(o.instance_commited, r.instance_commited) = u.instance_commited
),

instance_specs AS (
  SELECT *
  FROM (
    SELECT
      TRIM(LOWER(instance_commited)) AS instance_commited,
      TRIM(LOWER(region)) AS region,
      TRIM(LOWER(circle_id)) AS circle_id,
      vcpu,
      memory,
      ROW_NUMBER() OVER (
        PARTITION BY instance_commited, region, circle_id
        ORDER BY vcpu DESC, memory DESC
      ) AS rn
    FROM {{ ref('aws_rds_costs_by_sdk') }}
    WHERE vcpu IS NOT NULL AND memory IS NOT NULL
  ) t
  WHERE rn = 1
)

SELECT
  mc.month,
  mc.region,
  mc.circle_id,
  mc.instance_commited,
  mc.reservation_start_time,
  mc.reservation_end_time,
  mc.reservations,
  mc.used_instances,
  ROUND(mc.total_ondemand_cost, 4) AS total_ondemand_cost,
  ROUND(mc.total_reservation_cost, 4) AS total_reservation_cost,
  ispec.vcpu,
  ispec.memory,

  CASE
    WHEN (ispec.vcpu + ispec.memory) > 0 THEN
      ROUND(
        CASE
          WHEN mc.total_reservation_cost > 0 THEN mc.total_reservation_cost
          ELSE mc.total_ondemand_cost
        END / (ispec.vcpu + ispec.memory),
        6
      )
    ELSE NULL
  END AS unit_cost,

  CASE
    WHEN (ispec.vcpu + ispec.memory) > 0 THEN
      ROUND((mc.total_ondemand_cost - mc.total_reservation_cost) / (ispec.vcpu + ispec.memory), 6)
    ELSE NULL
  END AS relative_savings_per_unit,

  CASE
    WHEN mc.used_instances > mc.reservations THEN 'uso_excedente'
    WHEN mc.used_instances < mc.reservations THEN 'reserva_subutilizada'
    ELSE 'uso_equilibrado'
  END AS reserva_eficiencia

FROM merged_costs mc
LEFT JOIN instance_specs ispec
  ON mc.instance_commited = ispec.instance_commited
 AND mc.region = ispec.region
 AND mc.circle_id = ispec.circle_id
ORDER BY mc.month, mc.region, mc.circle_id, mc.instance_commited