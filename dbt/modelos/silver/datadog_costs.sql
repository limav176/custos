WITH

source AS (
    SELECT * FROM {{ ref('stg_datadog_cost_attribution') }}
),

aux_datadog_service_circle AS (
    SELECT * FROM {{ ref('aux_datadog_service_circle') }}
),

aux_datadog_product AS (
    SELECT * FROM {{ ref('aux_datadog_product') }}
),

circle AS (
    SELECT * FROM {{ ref('holaspirit_circles') }}
),

labels_kubernetes_report AS (
    SELECT MAX(circleid) AS circle_id, MAX(namespace) AS namespace FROM {{ ref('stg_labels_kubernetes_report') }} GROUP BY namespace
),

aux_datadog AS (
    SELECT
        s.month
        ,COALESCE(s.tags_service, NULL) AS service
        ,COALESCE(s.tags_aws_account, NULL) AS aws_account
        ,s.family_type as product_name
        ,s.billing_type as billing_type
        ,s.invoice_status
        ,CAST(s.cost AS DOUBLE) AS cost
        ,CASE 
            WHEN s.family_type = 'dbm_host' THEN 
                COALESCE(s.tags_circleid, '999999999999999999999999')
            ELSE 
                COALESCE(
                    NULLIF(lkr.circle_id, ''),    -- SEGUNDA: labels_kubernetes_report
                    NULLIF(adsc.circle_id, ''),   -- TERCEIRA: aux_datadog_service_circle
                    '999999999999999999999999'    -- FALLBACK: Not Tagged
                )
        END AS circle_id
    FROM
        source s
    LEFT JOIN (
        SELECT 
            circle_id, 
            namespace,
            ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY circle_id) as rn
        FROM aux_datadog_service_circle
    ) adsc ON (s.tags_service = 'ecr-' || adsc.namespace OR s.tags_service = adsc.namespace) 
           AND adsc.rn = 1
    LEFT JOIN (
        SELECT 
            circle_id, 
            namespace,
            ROW_NUMBER() OVER (PARTITION BY namespace ORDER BY circle_id) as rn
        FROM labels_kubernetes_report
    ) lkr ON (s.tags_service = 'ecr-' || lkr.namespace OR s.tags_service = lkr.namespace) 
           AND lkr.rn = 1
    WHERE
        s.billing_type <> 'total' AND
        s.family_type NOT LIKE '%_cost_sum' AND
        CAST(s.cost AS DOUBLE) > 0.0
)

SELECT
    DATE_TRUNC('month', CAST(ad.month AS TIMESTAMP)) as start_date
    ,ad.service
    ,CASE
	WHEN (circle.circle_id IS NULL) THEN '999999999999999999999999' -- caso não exista no holaspirit, Not Tagged
	ELSE ad.circle_id END
     AS circle_id
    ,CASE
	WHEN (circle.circle_id IS NULL) THEN 'stream_aligned' -- caso não exista no holaspirit, Not Tagged
	ELSE circle.type END
     AS circle_type
    ,ad.product_name
    ,COALESCE(adp.product_group,'others') as product_group
    ,ad.billing_type
    ,ad.invoice_status
    ,ad.cost
FROM
    aux_datadog ad
LEFT JOIN
    circle circle ON circle.circle_id = ad.circle_id
LEFT JOIN
    aux_datadog_product adp ON adp.product_name = ad.product_name
