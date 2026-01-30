WITH checksum_silver AS(
    SELECT
        start_date
        ,table_name
        ,circle_id
        ,aws_product_name
        ,cost
    FROM
        {{ ref('silver_checksum_aws') }}
),

-- GOLD tables
gold_tables AS (
    SELECT start_date
           ,'aws_costs_month_marketplace' as table_name
           ,'999999999999999999999999' AS circle_id
           ,aws_product_name
           ,cost
    FROM {{ ref('aws_costs_month_marketplace') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_month_shared' as table_name
           ,circle_id
           ,aws_product_name
           ,cost
    FROM {{ ref('aws_costs_month_shared') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_month_support' as table_name
           ,circle_id
           ,aws_product_name
           ,cost
    FROM {{ ref('aws_costs_month_support') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_month_usage' as table_name
           ,circle_id
           ,aws_product_name
           ,cost
    FROM {{ ref('aws_costs_month_usage') }}
    UNION ALL
    SELECT start_date
           ,'aws_costs_month_untagged' as table_name
           ,circle_id
           ,aws_product_name
           ,cost
    FROM {{ ref('aws_costs_month_untagged') }}
),

gold AS (
    SELECT
        DATE_TRUNC('month', start_date) AS start_date
        ,table_name
        ,circle_id
        ,aws_product_name
        ,SUM(cost) as cost
    FROM
        gold_tables
    GROUP BY 1,2,3,4
),

-- aws_invoice
invoice AS (
    SELECT
        month AS start_date
        ,'aws_invoice' AS table_name
        ,circle_id
        ,aws_product_name
        ,SUM(cost) as cost
    FROM
        {{ ref('aws_invoice') }}
    GROUP BY 1,2,3,4
),

-- circle_month (fyf)
fyf AS (
    SELECT
        month AS start_date
        ,'circle_month' AS table_name
        ,circle_id
        ,'' AS aws_product_name
        ,SUM(cost) as cost
    FROM
        {{ ref('circle_month') }}
    WHERE origin = 'AWS'
    GROUP BY 1,2,3,4
),

fyf_plus_marketplace AS (
    SELECT 
        start_date
        ,'circle_month_plus_marketplace' AS table_name
        ,circle_id
        ,'' AS aws_product_name
        ,cost
    FROM 
        fyf
    UNION ALL
    SELECT 
        start_date
        ,'circle_month_plus_marketplace' AS table_name
        ,'999999999999999999999999' AS circle_id
        ,aws_product_name
        ,cost
    FROM 
        {{ ref('aws_costs_month_marketplace') }}
)

-- SELECT
--     c.start_date,
--     ROUND(c.bronze_cost, 2) AS bronze_cost,
--     ROUND(c.stg_bronze_cost, 2) AS stg_bronze_cost,
--     ROUND(c.silver_cost, 2) AS silver_cost,
--     ROUND(g.gold_cost, 2) AS gold_cost,
--     ROUND(i.invoice_cost, 2) AS invoice_cost,
--     ROUND(f.cost, 2) AS fif_cost
-- FROM
--     checksum_silver c
-- LEFT JOIN
--     gold g ON c.start_date = g.start_date
-- LEFT JOIN
--     invoice i ON c.start_date = i.start_date
-- LEFT JOIN
--     fif f ON c.start_date = f.start_date;

SELECT * FROM checksum_silver
UNION ALL
SELECT * FROM gold
UNION ALL
SELECT * FROM invoice
UNION ALL
SELECT * FROM fyf_plus_marketplace 
