WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'mongodb_atlas_cost_usage_report') }}
),

staging AS (
	SELECT
		CAST(PARSE_DATETIME(startdate, 'yyyy-MM-dd''T''HH:mm:ssZ') as TIMESTAMP) as start_date
		,CAST(PARSE_DATETIME(enddate, 'yyyy-MM-dd''T''HH:mm:ssZ') as TIMESTAMP) as end_date
		,groupname
		,clustername
		,sku
		,unit
		,quantity
		,totalpricecents
		,unitpricedollars
		,CAST(json_extract(replace(tags, '''', '"'), '$.CircleId[0]') as varchar) AS tag_circle_id
		,CAST(json_extract(replace(tags, '''', '"'), '$.Circle[0]') as varchar) AS tag_circle
		,CAST(json_extract(replace(tags, '''', '"'), '$.Environment[0]') as varchar) AS tag_environment
	FROM
		source
)

SELECT 
    start_date
    ,end_date
    ,groupname
    ,clustername
    ,sku
    ,unit
    ,quantity
    ,totalpricecents
    ,unitpricedollars
	,CASE 
        WHEN LENGTH(tag_circle_id) <> 24 THEN NULL 
        ELSE tag_circle_id 
    END AS tag_circle_id
    ,tag_circle
    ,tag_environment
FROM
	staging