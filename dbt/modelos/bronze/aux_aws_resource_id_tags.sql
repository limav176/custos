-- TABELA PARA PODER BUSCAR AS TAGS MAIS RECENTES PARA CADA RESOURCE_ID
-- ATUALMENTE CORRELACIONA QUATRO TAG'S: Product, Repository, CircleID e Management
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT

WITH
aws AS (
    SELECT
        line_item_resource_id
        ,resource_tags_user_product
        ,resource_tags_user_repository
        ,resource_tags_user_circle_id
		,resource_tags_user_provided_by_id
        ,resource_tags_user_management
        ,resource_tags_user_name
        ,MAX(CAST(line_item_usage_start_date AS TIMESTAMP)) AS line_item_usage_start_date
    FROM
		{{ source('custos_cloud_bronze', 'aws_cost_usage_report') }}
    WHERE
        line_item_operation <> 'EKSPod-EC2'
        AND line_item_line_item_type <> 'DiscountedUsage'
        AND COALESCE(line_item_resource_id, '') <> ''
    GROUP BY 7,6,5,4,3,2,1
),

tags AS (
	SELECT
		line_item_resource_id
		,resource_tags_user_product
		,resource_tags_user_repository
		,resource_tags_user_circle_id
		,resource_tags_user_provided_by_id
		,resource_tags_user_management
		,resource_tags_user_name
		,MAX(line_item_usage_start_date) AS line_item_usage_start_date
	FROM
		aws
	WHERE
		COALESCE(resource_tags_user_product, '') <> ''
		OR COALESCE(resource_tags_user_repository, '') <> ''
		OR COALESCE(resource_tags_user_circle_id, '') <> ''
		OR COALESCE(resource_tags_user_provided_by_id, '') <> ''
		OR COALESCE(resource_tags_user_management, '') <> ''
		OR COALESCE(resource_tags_user_name, '') <> ''
	GROUP BY
		7,6,5,4,3,2,1
),

all_resources AS (
	SELECT
		line_item_resource_id
        ,MAX(CAST(line_item_usage_start_date AS TIMESTAMP)) AS line_item_usage_start_date
	FROM
		aws
	GROUP BY 1
),

final AS (
SELECT
	r.line_item_resource_id AS resource_id
	,tags.resource_tags_user_product AS tag_product
	,tags.resource_tags_user_repository AS tag_repository
	,tags.resource_tags_user_circle_id AS circle_id
	,tags.resource_tags_user_provided_by_id AS providedby_id
	,tags.resource_tags_user_management AS tag_management
	,resource_tags_user_name AS tag_name
FROM
	all_resources r
JOIN tags tags
	ON	tags.line_item_resource_id = r.line_item_resource_id AND tags.line_item_usage_start_date = r.line_item_usage_start_date
WHERE
	COALESCE(tags.resource_tags_user_product, '') <> ''
	OR COALESCE(tags.resource_tags_user_repository, '') <> ''
	OR COALESCE(tags.resource_tags_user_circle_id, '') <> ''
	OR COALESCE(tags.resource_tags_user_provided_by_id, '') <> ''
	OR COALESCE(tags.resource_tags_user_management, '') <> ''
	OR COALESCE(tags.resource_tags_user_name, '') <> ''
GROUP BY 7,6,5,4,3,2,1
)

SELECT 
	resource_id
	,tag_product
	,tag_repository
	,circle_id
	,providedby_id
	,tag_management
	,tag_name
FROM 
	final