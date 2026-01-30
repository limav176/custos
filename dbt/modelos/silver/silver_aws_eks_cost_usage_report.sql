WITH source AS (
    SELECT * FROM {{ ref('stg_aws_eks_cost_usage_report') }}
),

aws_account_names AS (
    SELECT * FROM {{ ref('aws_account_names') }}
),

aux_aws_resource_id_tags AS (
    SELECT * FROM {{ ref('aux_aws_resource_id_tags') }}
),

aux_aws_account_circle AS (
    SELECT * FROM {{ ref('aux_aws_account_circle') }}
),

aux_eks_namespace_circle AS (
    SELECT * FROM {{ ref('aux_eks_namespace_circle') }}
),

labels_kubernetes_report AS (
    SELECT * FROM {{ ref('stg_labels_kubernetes_report') }}
),

staging AS (
    SELECT
        s.start_date
        ,s.resource_id
        ,s.cluster_arn
        ,s.cluster_name
        ,s.parent_resource_id
		,rh.tag_product as parent_tag_product
		,rh.tag_repository as parent_tag_repository
		,rh.circle_id as parent_tag_circle_id
		,rh.providedby_id as parent_provided_by_id
		,rh.tag_management as parent_tag_management
		,rh.tag_name as parent_tag_name
        ,s.namespace
        ,s.billing_origin
        ,COALESCE(                      -- Precedencia:
            NULLIF(lkr.circleid, '')    -- tabela labels_kubernetes_report
            ,NULLIF(aac.circle_id, '')  -- tabela aux_aws_account_circle
            ,NULLIF(enc.circle_id, '')  -- tabela eks_namespace_circle
            ,'999999999999999999999999'
        ) AS circle_id
        ,CASE
            WHEN (NULLIF(lkr.circleid, '') IS NULL) THEN false
            ELSE true
        END AS tag_policy_compliant
        ,CASE
            WHEN (NULLIF(lkr.circleid, '') IS NULL) THEN false
            ELSE true 
        END AS tag_policy_compliant_actual
        ,s.account_id
        ,aan.account_name
        ,s.region
        ,CASE
            WHEN aan.account_name LIKE '%prod%' THEN 'production'
            WHEN aan.account_name LIKE '%dev%' THEN 'development'
            WHEN s.region = 'us-east-1' THEN 'development'
            WHEN s.region = 'sa-east-1' THEN 'production'
            ELSE 'production'
        END AS environment
        ,s.usage_type
        ,s.pricing_term
        ,s.aws_product_name
        ,s.split_cost
        ,s.unused_cost
    FROM
        source s
    LEFT JOIN
        aws_account_names aan ON s.account_id = aan.account_id
    LEFT JOIN
        aux_aws_resource_id_tags rh ON s.resource_id = rh.resource_id
    LEFT JOIN
        aux_aws_account_circle aac ON s.account_id = aac.account_id
    LEFT JOIN
        aux_eks_namespace_circle enc ON s.namespace = enc.namespace
    LEFT JOIN
        labels_kubernetes_report lkr ON s.cluster_name = lkr.cluster_name AND s.namespace = lkr.namespace
)

SELECT
	start_date
	,environment
	,account_id
	,account_name
	,region
	,resource_id
    ,parent_resource_id
    ,parent_tag_product
    ,parent_tag_repository
    ,parent_tag_circle_id
    ,parent_provided_by_id
    ,parent_tag_management
    ,parent_tag_name
	,billing_origin
    ,circle_id
    ,tag_policy_compliant
    ,tag_policy_compliant_actual
	,usage_type
	,pricing_term
	,aws_product_name
    ,cluster_arn
	,cluster_name
	,namespace
	,split_cost
	,unused_cost
FROM 
    staging
