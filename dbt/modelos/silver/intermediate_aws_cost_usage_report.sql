WITH source AS (
    SELECT * FROM {{ ref('stg_aws_cost_usage_report') }}
),

aws_account_names AS (
    SELECT * FROM {{ ref('aws_account_names') }}
),

aux_aws_resource_id_tags AS (
    SELECT * FROM {{ ref('aux_aws_resource_id_tags') }}
),

aux_aws_tag_name_circle AS (
    SELECT * FROM {{ ref('aux_aws_tag_name_circle') }}
),

aux_aws_account_circle AS (
    SELECT * FROM {{ ref('aux_aws_account_circle') }}
),

aux_aws_product_circle AS (
    SELECT * FROM {{ ref('aux_aws_product_circle') }}
),

aux_aws_resource_circle AS (
    SELECT * FROM {{ ref('aux_aws_resource_circle') }}
),

aux_aws_tag_circle_circle AS (
    SELECT * FROM {{ ref('aux_aws_tag_circle_circle') }}
),

aux_aws_tag_product_circle AS (
    SELECT * FROM {{ ref('aux_aws_tag_product_circle') }}
),

final AS (
SELECT
    s.start_date
    ,s.resource_id
    ,s.description
    ,s.aws_product_name
    ,COALESCE(                       -- Precedencia:
        NULLIF(apo.circle_id, '')    -- tabela aux_aws_product_circle
        ,NULLIF(rh.circle_id, '')    -- tabela aux_aws_resource_id_tags
        ,NULLIF(s.circle_id, '')     -- tabela aws_cost_usage_report
        ,NULLIF(act.circle_id, '')   -- tabela rivery_aux_aws_tag_circle_circle
        ,NULLIF(acco.circle_id, '')  -- tabela aux_aws_account_circle
        ,NULLIF(apt.circle_id, '')   -- tabela aux_aws_tag_product_circle
        ,NULLIF(awtnc.circle_id, '') -- tabela aux_aws_tag_name_circle
        ,NULLIF(arc.circle_id, '')   -- tabela aux_aws_resource_circle
        ,'999999999999999999999999'  -- Not Tagged
    ) AS circle_id
    ,COALESCE(                       -- Precedencia:
        NULLIF(rh.providedby_id, '') -- tabela aux_aws_resource_id_tags
        ,NULLIF(s.providedby_id,'')  -- tabela aws_cost_usage_report
        ,'999999999999999999999999'
    ) AS providedby_id
    ,CASE
        WHEN NULLIF(apo.circle_id, '') IS NOT NULL THEN 'aux_aws_product_circle' ELSE
        CASE WHEN NULLIF(rh.circle_id, '') IS NOT NULL THEN 'aux_aws_resource_id_tags' ELSE
        CASE WHEN NULLIF(s.circle_id, '') IS NOT NULL THEN 'aws_cost_usage_report' ELSE
            CASE WHEN NULLIF(act.circle_id, '') IS NOT NULL THEN 'rivery_aux_aws_tag_circle_circle' ELSE
                CASE WHEN NULLIF(acco.circle_id, '') IS NOT NULL THEN 'aux_aws_account_circle' ELSE
                    CASE WHEN NULLIF(apt.circle_id, '') IS NOT NULL THEN 'aux_aws_tag_product_circle' ELSE
                            CASE WHEN NULLIF(awtnc.circle_id, '') IS NOT NULL THEN 'aux_aws_tag_name_circle' ELSE NULL
                            END
                        END
                    END
                END
            END
        END
    END AS circle_id_source
    ,COALESCE(                        -- Precedencia:
        NULLIF(rh.tag_repository, '') -- tabela aux_aws_resource_id_tags
        ,NULLIF(s.tag_repository, '') -- tabela aws_cost_usage_report
        ,'Not Tagged'
    ) AS tag_repository
    ,COALESCE(                     -- Precedencia:
        NULLIF(rh.tag_product, '') -- tabela aux_aws_resource_id_tags
        ,NULLIF(s.tag_product, '') -- tabela aws_cost_usage_report
        ,'Not Tagged'
    ) AS tag_product
    ,COALESCE(                        -- Precedencia:
        NULLIF(rh.tag_management, '') -- tabela aux_aws_resource_id_tags
        ,NULLIF(s.tag_management, '') -- tabela aws_cost_usage_report
        ,'Not Tagged'
    ) AS tag_management
    ,COALESCE(                  -- Precedencia:
        NULLIF(rh.tag_name, '') -- tabela aux_aws_resource_id_tags
        ,NULLIF(s.tag_name, '') -- tabela aws_cost_usage_report
        ,'Not Tagged'
    ) AS tag_name
    ,(CASE
        WHEN s.aws_product_name IN ('Amazon GuardDuty', 'AWS Config', 'AWS Security Hub', 'Amazon Inspector', 'AWS CloudTrail', 'AWS Key Management Service', 'AWS Systems Manager') THEN true -- Bypass resources untaggable
        WHEN NULLIF(arc.circle_id, '') IS NOT NULL THEN true -- Bypass resources untaggable
        WHEN (NULLIF(rh.circle_id, '') IS NULL AND NULLIF(s.circle_id, '') IS NULL) THEN false
        ELSE true END
    ) AS tag_policy_compliant
    ,(CASE
        WHEN s.aws_product_name IN ('Amazon GuardDuty', 'AWS Config', 'AWS Security Hub', 'Amazon Inspector', 'AWS CloudTrail', 'AWS Key Management Service', 'AWS Systems Manager') THEN true -- Bypass resources untaggable
        WHEN NULLIF(arc.circle_id, '') IS NOT NULL THEN true -- Bypass resources untaggable
        WHEN (NULLIF(s.circle_id, '') IS NULL) THEN false
        ELSE true END
    ) AS tag_policy_compliant_actual
    ,s.billing_origin
    ,s.item_type
    ,s.account_id
    ,NULLIF(acc.account_name, '') AS account_name
    ,s.region
    ,CASE
        WHEN acc.account_name LIKE '%prod%' THEN 'production'
        WHEN acc.account_name LIKE '%dev%' THEN 'development'
        WHEN s.region = 'us-east-1' THEN 'development'
        WHEN s.region = 'sa-east-1' THEN 'production'
        ELSE 'production'
    END AS environment
    ,s.usage_type
    ,s.instance_type
    ,s.multi_az
    ,s.engine
    ,s.pricing_term
    ,s.unblended_cost
    ,s.reservation_cost
    ,s.reservation_start_time
    ,s.reservation_end_time
    ,s.reservation_net_effective_cost
    ,s.reservation_amortized_upfront_cost
FROM
    source s
LEFT JOIN
    aws_account_names acc ON s.account_id = acc.account_id
LEFT JOIN
    aux_aws_account_circle acco ON s.account_id = acco.account_id
LEFT JOIN
    aux_aws_product_circle apo ON s.aws_product_name = apo.product_name
LEFT JOIN
    aux_aws_tag_circle_circle act ON s.tag_circle = act.tag_circle
LEFT JOIN
    aux_aws_tag_product_circle apt ON s.tag_product = apt.tag_product
LEFT JOIN
    aux_aws_resource_circle arc ON s.resource_id = arc.resource_id
LEFT JOIN
    aux_aws_resource_id_tags rh ON s.resource_id = rh.resource_id
LEFT JOIN
    aux_aws_tag_name_circle awtnc ON s.tag_name = awtnc.tag_name
)

SELECT * FROM final