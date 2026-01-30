-- TABELA PARA PODER BUSCAR AS TAGS MAIS RECENTES PARA CADA RESOURCE
-- ATUALMENTE CORRELACIONA QUATRO TAG'S: CircleID, Circle, Enviroment
-- UTILIZADO NA TABELA: STG_MONGODB_ATLAS_COST_USAGE_REPORT

with source as (
    SELECT * FROM {{ source('custos_cloud_bronze', 'mongodb_atlas_cost_usage_report') }}
),

holaspirit as(
    SELECT * FROM {{ source('custos_cloud_bronze', 'rivery_holaspirit') }}
),

tags as (
    SELECT
    	clustername
		,CAST(json_extract(replace(tags, '''', '"'), '$.CircleId[0]') as varchar) AS tag_circle_id
		,CAST(json_extract(replace(tags, '''', '"'), '$.Circle[0]') as varchar) AS tag_circle
		,CAST(json_extract(replace(tags, '''', '"'), '$.Environment[0]') as varchar) AS tag_environment
        ,MAX(CAST(PARSE_DATETIME(startdate, 'yyyy-MM-dd''T''HH:mm:ssZ') as TIMESTAMP)) AS start_date
	FROM
		 source
    WHERE
        NULLIF(clustername, '') IS NOT NULL
    GROUP BY 4,3,2,1
),

all_resources AS (
	SELECT
		clustername
        ,MAX(CAST(PARSE_DATETIME(startdate, 'yyyy-MM-dd''T''HH:mm:ssZ') as TIMESTAMP)) AS start_date
	FROM
		source
	GROUP BY 1
),

final AS (
SELECT
	r.clustername
	,CASE
		WHEN h.holaspirit_name IS NULL THEN NULL -- caso não exista no holaspirit, Not Tagged
		ELSE tags.tag_circle_id END
	  AS tag_circle_id
	,tags.tag_circle
	,tags.tag_environment
FROM
	all_resources r
JOIN tags tags
	ON	tags.clustername = r.clustername
    AND tags.start_date = r.start_date
LEFT JOIN holaspirit h
    ON h.holaspirit_id = tags.tag_circle_id
WHERE
	COALESCE(tags.tag_circle_id, '') <> ''
	OR COALESCE(tags.tag_circle, '') <> ''
	OR COALESCE(tags.tag_environment, '') <> ''
GROUP BY 4,3,2,1
)

SELECT 
	clustername
	,tag_circle_id
	,tag_circle
	,tag_environment
FROM 
	final