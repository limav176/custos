WITH
aws_account_names AS (
    SELECT * FROM {{ ref('aws_account_names') }}
),

stg_cloudwatch_rds_report AS (
SELECT *
FROM {{ ref('stg_cloudwatch_rds_report') }}
WHERE
    DATE_TRUNC('day', start_date) > (current_date - interval '30' day)
    AND DATE_TRUNC('day', start_date) <> current_date
),

resource_details AS (
SELECT
    start_date
    ,account_id
    ,region
    ,instance_name
    ,resource_id
    ,instance_size
    ,'Amazon Relational Database Service' AS aws_product_name
FROM
    stg_cloudwatch_rds_report
GROUP BY 6,5,4,3,2,1
),

latest_resource_details AS (
SELECT
    account_id
    ,region
    ,instance_name
    ,resource_id
    ,aws_product_name
    ,MAX(instance_size) AS instance_size
FROM
    resource_details
GROUP BY 5,4,3,2,1
),

rds_metrics AS (
SELECT
    start_date
    ,resource_id
    ,instance_size
    ,engine
    ,metric_name
    ,metric_statistics
    ,metric_value
FROM
   stg_cloudwatch_rds_report
),

rds_costs AS (
SELECT
    start_date
    ,resource_id
    ,circle_id
    ,SUM(CASE WHEN usage_type = 'Extended Support' THEN total_cost ELSE 0.0 END) AS extended_support_cost
    ,SUM(CASE WHEN usage_type = 'Usage' THEN total_cost ELSE 0.0 END) AS usage_cost
    ,SUM(total_cost) AS total_cost
FROM
    {{ ref('aws_rds_costs_by_sdk') }}
WHERE
    DATE_TRUNC('day', start_date) > (current_date - interval '30' day)
    AND DATE_TRUNC('day', start_date) <> current_date
GROUP BY
    3,2,1
),

wi_without_connections AS (
SELECT
    start_date
    ,resource_id
    ,'Without Connections' AS type_waste_index
    ,CASE WHEN MAX(metric_value) = 0 THEN 1.0 ELSE 0.0 END AS waste_index
FROM
    rds_metrics
WHERE
    metric_name = 'DatabaseConnections'
    AND metric_statistics = 'Maximum'
GROUP BY
    3,2,1
),

wi_resource_underutilized AS (
SELECT
    rm.start_date
    ,rm.resource_id
    ,'Resource Underutilized' AS type_waste_index
    ,CASE
        WHEN rm.engine IN ('postgresql', 'mysql') AND rm.instance_size IN ('db.t3.micro') THEN 0.0
        WHEN rm.engine IN ('aurora-postgresql', 'aurora-mysql') AND rm.instance_size IN ('db.t4g.medium','db.t3.medium', 'db.r6g.large', 'db.r7g.large') THEN 0.0
        WHEN rm.instance_size = 'db.serverless' THEN (SUM(rc.usage_cost)/SUM(rc.total_cost)) * ((100.0 - MAX(rm.metric_value)) / 100.0) * 0.5
        WHEN ((100.0 - MAX(rm.metric_value)) / 100.0) > 0.75 THEN (SUM(rc.usage_cost)/SUM(rc.total_cost))*0.75
        WHEN ((100.0 - MAX(rm.metric_value)) / 100.0) > 0.5  THEN (SUM(rc.usage_cost)/SUM(rc.total_cost))*0.5
        ELSE 0.0
    END AS waste_index
FROM
    rds_metrics rm
LEFT JOIN
    rds_costs rc ON rm.resource_id = rc.resource_id AND rm.start_date = rc.start_date
WHERE
    rm.metric_name = 'CPUUtilization'
    AND rm.metric_statistics = 'Average'
    AND rc.total_cost > 0
GROUP BY
    2,1,rm.engine,rm.instance_size
),

wi_extended_support AS (
SELECT
    start_date
    ,resource_id
    ,'Extended Support' AS type_waste_index
    ,CASE WHEN SUM(total_cost) > 0 AND SUM(extended_support_cost) > 0
        THEN SUM(extended_support_cost)/SUM(total_cost)/1.0
        ELSE 0.0
    END AS waste_index
FROM
    rds_costs
GROUP BY
    3,2,1
),

partial_waste_index AS (
SELECT * FROM wi_without_connections
UNION ALL
SELECT * FROM wi_resource_underutilized
UNION ALL
SELECT * FROM wi_extended_support
),

waste_index_p95 AS (
SELECT
    resource_id
    ,type_waste_index
    ,approx_percentile(waste_index, 0.95) AS waste_index
FROM
    partial_waste_index
WHERE
    DATE_TRUNC('day', start_date) > (current_date - interval '8' day)
GROUP BY 2,1
),

ordered_recommendation AS (
    SELECT
        p95.resource_id,
        CASE
            WHEN p95.type_waste_index = 'Without Connections' AND p95.waste_index = 1.0 THEN 'Shutdown or terminate the instance, as it has had no connections'
            WHEN p95.type_waste_index = 'Resource Underutilized' AND p95.waste_index > 0.25 AND lrd.instance_size = 'db.serverless' THEN 'Reduce the minimum capacity ACUs due to low utilization'
            WHEN p95.type_waste_index = 'Resource Underutilized' AND p95.waste_index > 0.5 AND lrd.instance_size NOT LIKE 'db.%g.%' THEN 'Reduce the instance size due to low utilization - Change instance size to graviton'
            WHEN p95.type_waste_index = 'Resource Underutilized' AND p95.waste_index > 0.5 THEN 'Reduce the instance size due to low utilization'
            WHEN p95.type_waste_index = 'Resource Underutilized' AND lrd.instance_size NOT LIKE 'db.%g.%' THEN 'Change instance size to graviton'
            WHEN p95.type_waste_index = 'Extended Support' AND p95.waste_index > 0.0 THEN 'Update the engine version due to Extended Support'
            ELSE NULL
        END AS recommendation,
        CASE
            WHEN p95.type_waste_index = 'Without Connections' THEN 1
            WHEN p95.type_waste_index = 'Extended Support' THEN 2
            WHEN p95.type_waste_index = 'Resource Underutilized' THEN 3
            ELSE 4
        END AS recommendation_type_order
    FROM
        waste_index_p95 p95
    LEFT JOIN
        latest_resource_details lrd ON lrd.resource_id = p95.resource_id
    WHERE
        p95.waste_index > 0
    GROUP BY 3,2,1
),

recommendation AS (
    SELECT
        resource_id,
        ARRAY_JOIN(ARRAY_AGG(recommendation ORDER BY recommendation_type_order), ' - ') AS recommendation
    FROM
        ordered_recommendation
    WHERE
        recommendation IS NOT NULL
    GROUP BY
        resource_id
),

aggregated_waste_index AS (
SELECT
    start_date
    ,resource_id
    ,'Aggregated' AS type_waste_index
    ,CASE
        WHEN MAX(type_waste_index) = 'Without Connections' AND MAX(waste_index) = 1.0 THEN 1.0
        ELSE SUM(waste_index)
    END AS waste_index
FROM
    partial_waste_index
GROUP BY 2,1
ORDER BY 1 ASC
),

final AS (
SELECT
    rd.start_date
    ,rd.account_id
    ,NULLIF(acc.account_name, '') AS account_name
    ,CASE
            WHEN acc.account_name LIKE '%prod%' THEN 'production'
            WHEN acc.account_name LIKE '%dev%' THEN 'development'
            WHEN rd.region = 'us-east-1' THEN 'development'
            WHEN rd.region = 'sa-east-1' THEN 'production'
            ELSE 'production'
        END AS environment
    ,rd.region
    ,rc.circle_id
    ,rd.aws_product_name
    ,rd.instance_name
    ,rd.resource_id
    ,rd.instance_size
    ,rec.recommendation
    --,ROUND(approx_percentile(CASE WHEN type_waste_index = 'Extended Support' THEN waste_index ELSE 0.0 END,0.95),2) AS wi_extended_support
    --,ROUND(approx_percentile(CASE WHEN type_waste_index = 'Resource Underutilized' THEN waste_index ELSE 0.0 END,0.95),2) AS wi_resource_underutilized
    --,ROUND(approx_percentile(CASE WHEN type_waste_index = 'Without Connections' THEN waste_index ELSE 0.0 END,0.95),2) AS wi_without_connections
    ,ROUND(approx_percentile(CASE WHEN type_waste_index = 'Aggregated' THEN waste_index ELSE 0.0 END,0.95),2) AS waste_index
    ,CAST(SUM(CASE WHEN awi.type_waste_index = 'Aggregated' THEN rc.total_cost ELSE 0.0 END) * approx_percentile(CASE WHEN awi.type_waste_index = 'Aggregated' THEN awi.waste_index ELSE 0.0 END,0.95) AS DOUBLE) AS waste_cost
    ,CAST(SUM(CASE WHEN awi.type_waste_index = 'Aggregated' THEN rc.total_cost ELSE 0.0 END) AS DOUBLE) AS total_cost
FROM
    resource_details rd
LEFT JOIN
    aggregated_waste_index awi ON rd.start_date = awi.start_date AND rd.resource_id = awi.resource_id
LEFT JOIN
    rds_costs rc ON rc.start_date = rd.start_date AND rc.resource_id = rd.resource_id
LEFT JOIN
    aws_account_names acc ON rd.account_id = acc.account_id
LEFT JOIN
    recommendation rec ON rec.resource_id = rd.resource_id
WHERE
    rc.total_cost > 0
GROUP BY
    11,10,9,8,7,6,5,3,2,1,acc.account_name
ORDER BY
    start_date ASC
)

SELECT * FROM final
