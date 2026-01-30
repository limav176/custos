WITH
aws AS (
    SELECT * FROM {{ ref('intermediate_aws_cost_usage_report') }} 
    WHERE aws_product_name = 'Amazon Relational Database Service'
),
by_sdk AS (
SELECT * FROM {{ ref('aws_rds_costs_by_sdk') }} 
WHERE 
    COALESCE(usage_type, '') = 'Usage' 
    AND COALESCE(instance_size, '') <> 'db.serverless' 
    AND YEAR(start_date) >= 2025 
    AND COALESCE(region, '') = 'sa-east-1'
),
aws_costs AS (
SELECT
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    ,resource_id
    ,description
    ,aws_product_name
    ,circle_id
    ,providedby_id
    ,circle_id_source
    ,tag_repository
    ,tag_product
    ,tag_management
    ,tag_name
    ,tag_policy_compliant
    ,tag_policy_compliant_actual
    ,billing_origin
    ,item_type
    ,account_id
    ,account_name
    ,region
    ,environment
    ,usage_type
    ,instance_type
    ,multi_az
    ,engine
    ,pricing_term
    ,reservation_start_time
    ,reservation_end_time
    ,SUM(unblended_cost) AS unblended_cost
    ,SUM(reservation_cost) AS reservation_cost
    ,SUM(reservation_net_effective_cost) AS reservation_net_effective_cost
    ,SUM(reservation_amortized_upfront_cost) AS reservation_amortized_upfront_cost
FROM
    aws
WHERE
    NOT (
        YEAR(start_date) >= 2025
        AND COALESCE(region,'') = 'sa-east-1'
        AND (
            (
                COALESCE(usage_type,'') LIKE '%InstanceUsage:db.%'
                OR COALESCE(usage_type,'') LIKE '%Multi-AZUsage:db.%'
                OR COALESCE(usage_type,'') LIKE '%InstanceUsageIOOptimized:db.%'
            )
            OR
            (
                COALESCE(pricing_term,'') = 'Reserved'
                AND COALESCE(account_id,'') = '887592687927'
                AND COALESCE(usage_type,'') LIKE '%HeavyUsage:db.%'
            )
        )
    )
    GROUP BY
        26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
aws_rds_costs_by_sdk AS (
SELECT
    DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
    ,resource_id
    ,'Engine: ' || engine || ', Size: ' || instance_size || ', MultiAZ: ' || multi_az AS description
    ,'Amazon Relational Database Service' AS aws_product_name
    ,circle_id
    ,'' AS providedby_id
    ,'aws_rds_costs_by_sdk' AS circle_id_source
    ,tag_repository
    ,tag_product
    ,tag_management
    ,instance_name AS tag_name
    ,TRUE AS tag_policy_compliant
    ,TRUE AS tag_policy_compliant_actual
    ,'AWS' AS billing_origin
    ,'Usage' AS item_type
    ,account_id
    ,account_name
    ,region
    ,CASE
        WHEN account_name LIKE '%prod%' THEN 'production'
        WHEN account_name LIKE '%dev%' THEN 'development'
        WHEN region = 'us-east-1' THEN 'development'
        WHEN region = 'sa-east-1' THEN 'production'
        ELSE 'production'
    END AS environment
    ,usage_type
    ,instance_size AS instance_type
    ,multi_az
    ,engine
    ,'' AS pricing_term
    ,CAST(NULL AS TIMESTAMP) AS reservation_start_time
    ,CAST(NULL AS TIMESTAMP) AS reservation_end_time
    ,SUM(total_cost) AS unblended_cost
    ,0.0 AS reservation_cost
    ,0.0 AS reservation_net_effective_cost
    ,0.0 AS reservation_amortized_upfront_cost
FROM
    by_sdk
GROUP BY
    26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
savings AS (
SELECT
    month AS start_date
    ,'Savings - Reservation - ' || engine || ' - ' || instance_commited AS resource_id
    ,'Savings Reservation - Engine: ' || engine
        || ', Size Commited: ' || instance_commited
        || ', Instances: ' || CAST(instances AS VARCHAR)
        || ', Instances Actives: ' || CAST(instances_inactives AS VARCHAR)
        ||  ', Reservations: ' || CAST(reservations AS VARCHAR)
        || ', Instance Hrs: ' || CAST(instance_hours AS VARCHAR)
        || ', Reservation Hrs: ' || CAST(reservation_hours AS VARCHAR)
        || ', Ondemand Hrs: ' || CAST(ondemand_hours AS VARCHAR)
        || ', Reservation Cost: ' || CAST(reservation_cost AS VARCHAR)
        || ', Ondemand Cost: ' || CAST(ondemand_cost AS VARCHAR)
    AS description
    ,'Amazon Relational Database Service' AS aws_product_name
    ,circle_id
    ,'' AS providedby_id
    ,'silver_aws_rds_reservations_costs' AS circle_id_source
    ,'' AS tag_repository
    ,'' AS tag_product
    ,'' AS tag_management
    ,'RDS - Savings Reservations' AS tag_name
    ,TRUE AS tag_policy_compliant
    ,TRUE AS tag_policy_compliant_actual
    ,'AWS' AS billing_origin
    ,'Usage' AS item_type
    ,'887592687927' AS account_id
    ,'master' AS account_name
    ,region
    ,CASE
        WHEN region = 'us-east-1' THEN 'development'
        WHEN region = 'sa-east-1' THEN 'production'
        ELSE 'production'
    END AS environment
    ,'Usage' AS usage_type
    ,instance_commited AS instance_type
    ,'' AS multi_az
    ,engine
    ,'' AS pricing_term
    ,CAST(NULL AS TIMESTAMP) AS reservation_start_time
    ,CAST(NULL AS TIMESTAMP) AS reservation_end_time
    ,SUM(savings)*-1 AS unblended_cost
    ,0.0 AS reservation_cost
    ,0.0 AS reservation_net_effective_cost
    ,0.0 AS reservation_amortized_upfront_cost
FROM
    {{ ref('silver_aws_rds_reservations_costs') }} 
WHERE
    savings > 0
    AND YEAR(month) >= 2025
GROUP BY
    26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
---- DIFF
primary_union AS (
SELECT * FROM aws_costs
    UNION ALL
SELECT * FROM aws_rds_costs_by_sdk
    UNION ALL
SELECT * FROM savings
),
costs_primary_union AS (
SELECT
    DATE_TRUNC('month', start_date) AS month
    ,region
    ,'primary_union' AS table_name
    ,SUM(unblended_cost + reservation_cost) AS total_cost
FROM
    primary_union
WHERE 
    YEAR(start_date) >= 2025
GROUP BY 3,2,1
),
all_costs_aws_rds AS (
SELECT
    DATE_TRUNC('month', start_date) AS month
    ,region
    ,'aws' AS table_name
    ,SUM(unblended_cost + reservation_cost) AS total_cost
FROM
    aws
WHERE 
    YEAR(start_date) >= 2025
GROUP BY 3,2,1
),
diff_union AS (
SELECT * FROM costs_primary_union
    UNION ALL
SELECT * FROM all_costs_aws_rds
),
diff AS (
SELECT
    month
    ,region
    ,SUM(TABLE_AWS - TABLE_NEW) AS diff
FROM (
    SELECT
        month
        ,region
        ,SUM(CASE WHEN table_name = 'primary_union' THEN total_cost ELSE 0.0 END) AS TABLE_NEW
        ,SUM(CASE WHEN table_name = 'aws' THEN total_cost ELSE 0.0 END) AS TABLE_AWS
    FROM diff_union
    GROUP BY 2,1
    )
GROUP BY 2,1
),
divergence AS (
SELECT
    month AS start_date
    ,'Difference - Reservations' AS resource_id
    ,'Divergencia de custos de Reservas, esse custo tende a reduzir ate o fechamento' AS description
    ,'Amazon Relational Database Service' AS aws_product_name
    ,'62992fc2c997c81650231188' AS circle_id
    ,'' AS providedby_id
    ,'silver_aws_rds_costs' AS circle_id_source
    ,'' AS tag_repository
    ,'' AS tag_product
    ,'' AS tag_management
    ,'RDS - Difference Reservations' AS tag_name
    ,TRUE AS tag_policy_compliant
    ,TRUE AS tag_policy_compliant_actual
    ,'AWS' AS billing_origin
    ,'Usage' AS item_type
    ,'887592687927' AS account_id
    ,'master' AS account_name
    ,region
    ,CASE
        WHEN region = 'us-east-1' THEN 'development'
        WHEN region = 'sa-east-1' THEN 'production'
        ELSE 'production'
    END AS environment
    ,'Usage' AS usage_type
    ,'' AS instance_type
    ,'' AS multi_az
    ,'' AS engine
    ,'' AS pricing_term
    ,CAST(NULL AS TIMESTAMP) AS reservation_start_time
    ,CAST(NULL AS TIMESTAMP) AS reservation_end_time
    ,SUM(diff) AS unblended_cost
    ,0.0 AS reservation_cost
    ,0.0 AS reservation_net_effective_cost
    ,0.0 AS reservation_amortized_upfront_cost
FROM
    diff
GROUP BY
    month, region
),
final_union AS (
SELECT * FROM primary_union
    UNION ALL
SELECT * FROM divergence
)
SELECT * FROM final_union