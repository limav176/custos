--AWS Costs
SELECT
    'AWS' as origin
    ,start_date
    ,circle_id
    ,'usage' as type
    ,SUM(cost) as cost
FROM
    {{ ref('aws_costs_month_usage') }}
GROUP BY 1,2,3,4

UNION ALL

SELECT
    'AWS' as origin
    ,start_date
    ,circle_id
    ,'untagged' as type
    ,SUM(cost) as cost
FROM
    {{ ref('aws_costs_month_untagged') }}
GROUP BY 1,2,3,4

UNION ALL

SELECT
    'AWS' as origin
    ,start_date
    ,circle_id
    ,'support'
    ,SUM(cost) as cost
FROM
    {{ ref('aws_costs_month_support') }}
GROUP BY 1,2,3,4

UNION ALL

SELECT
    'AWS' as origin
    ,start_date
    ,circle_id
    ,'shared'
    ,SUM(cost) as cost
FROM
    {{ ref('aws_costs_month_shared') }}
GROUP BY 1,2,3,4

--MongoDB Atlas Costs
UNION ALL

SELECT
    'MongoDB Atlas' as origin
    ,start_date
    ,circle_id
    ,'support'
    ,SUM(cost) as cost
FROM
    {{ ref('mongodb_atlas_costs_month_support') }}
GROUP BY 1,2,3,4

UNION ALL

SELECT
    'MongoDB Atlas' as origin
    ,start_date
    ,circle_id
    ,'usage'
    ,SUM(cost) as cost
FROM
    {{ ref('mongodb_atlas_costs_month_usage') }}
GROUP BY 1,2,3,4

--Datadog Atlas Costs
UNION ALL

SELECT
    'Datadog' as origin
    ,start_date
    ,circle_id
    ,CASE
	    WHEN invoice_status = 'closed' THEN 'shared'
	    ELSE invoice_status END
     AS type
    ,SUM(cost) as cost
FROM
    {{ ref('datadog_costs_month_shared') }}
GROUP BY 1,2,3,4

UNION ALL

SELECT
    'Datadog' as origin
    ,start_date
    ,circle_id
    ,CASE
	    WHEN invoice_status = 'closed' THEN 'usage'
	    ELSE invoice_status END
     AS type
    ,SUM(cost) as cost
FROM
    {{ ref('datadog_costs_month_usage') }}
GROUP BY 1,2,3,4
