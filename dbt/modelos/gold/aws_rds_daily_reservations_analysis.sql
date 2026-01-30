WITH source AS (
    SELECT * FROM {{ ref('aws_rds_daily_reservations_instances') }}
),

final AS (
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
        without_reservations,
        -- Taxa de utilização das reservas
        CASE 
            WHEN instances > 0 THEN 
                CAST((reservations * 100.0 / instances) AS DECIMAL(10,2))
            ELSE 0.0 
        END AS reservation_utilization_rate,
        -- Status da reserva
        CASE 
            WHEN instances = 0 AND reservations > 0 THEN 'Over-reserved'
            WHEN instances > 0 AND reservations = 0 THEN 'Under-reserved'
            WHEN instances > 0 AND reservations > 0 AND without_reservations > 0 THEN 'Under-reserved'
            WHEN instances > 0 AND reservations > 0 AND without_reservations < 0 THEN 'Over-reserved'
            WHEN instances > 0 AND reservations > 0 AND without_reservations = 0 THEN 'Optimal'
            ELSE 'No instances'
        END AS reservation_status
    FROM source
    WHERE start_date IS NOT NULL
)

SELECT * FROM final
ORDER BY start_date DESC, region, engine, multi_az, instance_commited, circle_id 