WITH
circle AS (
    SELECT * FROM {{ ref('holaspirit_circles') }}
)

,tag_product(cluster_name, tag_product) as (
	VALUES
		('will-dev-test','EKS - Cluster Test')
		,('will-prod-shared','EKS - Cluster Shared')
		,('will-dev-shared','EKS - Cluster Shared')
		,('will-prod-foundation-platforms','EKS - Cluster Foundation Platforms')
		,('will-dev-foundation-platforms','EKS - Cluster Foundation Platforms')
		,('analytics-prod-credit-decision','EKS - Credit Decision')
		,('analytics-dev-credit-decision','EKS - Credit Decision')
		,('eks-will-prod-01','EKS - Legacy Cluster')
		,('eks-will-dev-02','EKS - Legacy Cluster')
)

,stream_aligned_cost AS (
    SELECT
        eks.start_date
        ,eks.namespace
		,eks.cluster_name
    	,eks.parent_resource_id as instance_id
        ,'Kubernetes' description
    	,eks.billing_origin
    	,eks.aws_product_name
    	,eks.circle_id
		,'999999999999999999999999' as providedby_id
		,eks.tag_policy_compliant
		,eks.tag_policy_compliant_actual
    	,circle.type AS circle_type
	    ,'Not Tagged' AS tag_name
    	,'Not Tagged' AS tag_repository
    	,'Not Tagged' AS tag_product
    	,'Not Tagged' AS tag_management
    	,eks.environment
    	,eks.usage_type
    	,eks.pricing_term
    	,eks.account_id
    	,eks.account_name
    	,eks.region
        ,eks.split_cost
    	,eks.unused_cost
    FROM
    	{{ ref('silver_aws_eks_cost_usage_report') }} eks
    LEFT JOIN
    	circle circle ON circle.circle_id = eks.circle_id
)

,platform_discount AS (
    SELECT
        eks.start_date
		,'empty' as namespace
        ,eks.cluster_name
    	,eks.parent_resource_id as instance_id
        ,'Discount related to Kubernetes ' || eks.cluster_name as description
    	,eks.billing_origin
    	,'Amazon Elastic Compute Cloud' as aws_product_name
    	-- ,CASE
    	--     WHEN eks.account_id='849517169598' THEN '6408d55611798f10f203fe5b' --cluster analytics -> Data Platform
    	--     ELSE '65a196d5a55f7b068f05df9c' END -- else -> InfraReliability
    	--     as circle_id
		-- ,'65a196d5a55f7b068f05df9c' as circle_id     --InfraReliability
		-- ,'65a196d5a55f7b068f05df9c' as providedby_id --InfraReliability
		,COALESCE(
			 NULLIF(eks.parent_tag_circle_id, '')
			 ,'65a196d5a55f7b068f05df9c') -- garantir consist de nodes não tagueados
		as circle_id
		,COALESCE(
			 NULLIF(eks.parent_provided_by_id, '')
			 ,'65a196d5a55f7b068f05df9c') -- garantir consist de nodes não tagueados
		as providedby_id
		,true as tag_policy_compliant
		,true as tag_policy_compliant_actual
    	,'platform' AS circle_type
		,eks.parent_tag_name as tag_name
		,eks.parent_tag_repository as tag_repository
		,COALESCE(
			 NULLIF(eks.parent_tag_product, '')
			 ,NULLIF(tp.tag_product, '')) -- garantir consist de nodes não tagueados
		as tag_product
		,eks.parent_tag_management as tag_management
    	-- ,'Not Tagged' AS tag_repository
    	-- ,COALESCE(tp.tag_product,'Not Tagged') AS tag_product
    	-- ,'Not Tagged' AS tag_management
    	,eks.environment
    	,'Discount related to Kubernetes ' || eks.cluster_name as usage_type
    	,eks.pricing_term
    	,eks.account_id
    	,eks.account_name
    	,eks.region
        ,eks.split_cost * (-1)
    	,eks.unused_cost * (-1)
    FROM
    	{{ ref('silver_aws_eks_cost_usage_report') }} eks
    LEFT JOIN
    	circle circle ON circle.circle_id = eks.circle_id
	LEFT JOIN
		tag_product tp ON tp.cluster_name = eks.cluster_name
)

SELECT * FROM stream_aligned_cost
UNION ALL
SELECT * FROM platform_discount
