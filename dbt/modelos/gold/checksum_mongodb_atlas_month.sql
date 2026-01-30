WITH silver_checksum AS (
    SELECT
        start_date,
        table_name,
        circle_id,
        sku,
        cost
    FROM
        {{ ref('silver_checksum_atlas') }}
)

,gold_tables AS (
    SELECT
        start_date,
        'mongodb_atlas_costs_month_total' AS table_name,
        circle_id,
        sku,
        SUM(cost) AS cost
    FROM (
        SELECT
            start_date,
            circle_id,
            sku,
            cost
        FROM {{ ref('mongodb_atlas_costs_month_usage') }}

        UNION ALL

        SELECT
            start_date,
            circle_id,
            sku,
            cost
        FROM {{ ref('mongodb_atlas_costs_month_support') }}
    ) AS combined_costs
    GROUP BY 1, 2, 3, 4
)
,invoice AS (
    SELECT
        month AS start_date,
        'mongodb_atlas_invoice' AS table_name,
        circle_id,
        sku,
        SUM(cost) AS cost
    FROM
        {{ ref('mongodb_atlas_invoice') }}
    GROUP BY 4, 3, 2, 1
)
,fif AS (
    SELECT
        month AS start_date,
        'circle_month' AS table_name,
        '' AS circle_id,
        '' AS sku,
        SUM(cost) AS cost
    FROM
        {{ ref('circle_month' ) }}
    WHERE
        origin = 'MongoDB Atlas'
    GROUP BY 4, 3, 2, 1
)

SELECT * FROM silver_checksum
UNION ALL
SELECT * FROM gold_tables
UNION ALL
SELECT * FROM invoice
UNION ALL
SELECT * FROM fif

