WITH bronze_checksum AS (
    SELECT
        start_date,
        table_name,
        circle_id,
        sku,
        cost
    FROM
        {{ ref('stg_checksum_atlas') }}
),

staging_checksum AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        'stg_mongodb_atlas_cost_usage_report' AS table_name,
        sku,
        COALESCE(tag_circle_id, 'unknown_circle') AS circle_id,
        CAST(SUM(CAST(totalpricecents AS DECIMAL(10,2))) / 100 AS DECIMAL(10,2)) AS cost
    FROM
        {{ ref('stg_mongodb_atlas_cost_usage_report') }}
    GROUP BY 1, 2, 3, 4
),

silver_tables AS (
        SELECT
            DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
            'silver_mongodb_atlas_cost_usage_report' AS table_name,
            sku,
            COALESCE(circle_id, 'unknown_circle') AS circle_id,
            CAST(SUM(CAST(totalpricecents AS DECIMAL(10,2))) / 100 AS DECIMAL(10,2)) AS cost
        FROM
            {{ ref('silver_mongodb_atlas_cost_usage_report') }}
        GROUP BY 1, 2, 3, 4

        UNION ALL

        SELECT
            start_date,
            'mongodb_atlas_costs_total' AS table_name,
            circle_id,
            sku,
            SUM(total_price_cents) AS cost
        FROM (
        SELECT
            start_date,
            circle_id,
            sku,
            total_price_cents
        FROM {{ ref('mongodb_atlas_costs_usage') }}

        UNION ALL

        SELECT
            start_date,
            circle_id,
            sku,
            total_price_cents
        FROM {{ ref('mongodb_atlas_costs_support') }}
    ) AS combined_costs
    GROUP BY 1, 2, 3, 4
),

silver AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS start_date,
        table_name,
        circle_id,
        sku,
        SUM(cost) AS cost
    FROM
        silver_tables
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM bronze_checksum
UNION ALL
SELECT * FROM staging_checksum
UNION ALL
SELECT * FROM silver
