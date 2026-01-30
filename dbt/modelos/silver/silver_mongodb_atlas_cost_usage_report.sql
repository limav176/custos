WITH staging AS (
    SELECT * FROM {{ ref('stg_mongodb_atlas_cost_usage_report') }}
),

aux_mongodb_atlas_tag_circle_circle AS (
    SELECT * FROM {{ ref('aux_mongodb_atlas_tag_circle_circle') }}
),

aux_mongodb_atlas_sku_circle AS (
    SELECT * FROM {{ ref('aux_mongodb_atlas_sku_circle') }}
),

aux_mongodb_atlas_untagged_clusters_circle AS (
    SELECT * FROM {{ ref('aux_mongodb_atlas_untagged_clusters_circle') }}
),

aux_mongodb_atlas_resource_tags AS (
    SELECT * FROM {{ ref('aux_mongodb_atlas_resource_tags') }}
)

SELECT
	s.start_date
	,s.end_date
	,s.clustername
	,s.sku
	,s.unit
	,s.quantity
	,s.totalpricecents
	,s.unitpricedollars
    ,COALESCE(                         -- Precedencia:
		NULLIF(s.tag_circle_id, '')    -- tabela mongodb_atlas_cost_usage_report - recurso tagueado
		,NULLIF(lst.tag_circle_id, '')  -- tabela aux_mongodb_atlas_resource_tags - últimas tags
		,NULLIF(act.circle_id, '')     -- tabela aux_mongodb_atlas_tag_circle_circle
		,NULLIF(msc.circle_id, '')     -- tabela aux_mongodb_atlas_sku_circle
		,NULLIF(untg.circle_id, '')    -- tabela aux_mongodb_atlas_untagged_clusters_circle
        ,'999999999999999999999999'    -- Not Tagged
    ) AS circle_id
    ,CASE
        WHEN (NULLIF(s.tag_circle_id, '') IS NULL AND NULLIF(lst.tag_circle_id, '') IS NULL) THEN false
        ELSE true
	END AS tag_policy_compliant
    ,CASE
        WHEN s.groupname LIKE '%prod%' THEN 'production'
        WHEN s.groupname LIKE '%dev%' THEN 'development'
        WHEN s.tag_environment = '%prod%' THEN 'production'
        WHEN s.tag_environment = '%dev%' THEN 'development'
        ELSE 'production'
    END AS environment
FROM
	staging s
LEFT JOIN
    aux_mongodb_atlas_tag_circle_circle act ON s.tag_circle = act.tag_circle
LEFT JOIN
    aux_mongodb_atlas_sku_circle msc ON s.sku = msc.sku
LEFT JOIN
    aux_mongodb_atlas_untagged_clusters_circle untg ON s.clustername = untg.cluster_name
LEFT JOIN
    aux_mongodb_atlas_resource_tags lst ON s.clustername = lst.clustername
