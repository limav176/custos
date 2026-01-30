-- custos de athena
WITH athena_costs AS
(
  SELECT
      DATE_TRUNC('day', CAST(start_date AS TIMESTAMP)) AS start_date
      ,account_id AS account_id
      ,environment AS environment
      ,account_name AS account_name
      ,region AS region
      ,billing_origin AS billing_origin
      ,split(resource_id, '/')[2] AS workgroup
      ,aws_product_name AS aws_product_name
      ,COALESCE(circle_id,'6408d55611798f10f203fe5b') AS circle_id --fallback DataPlat, custos de Dev
      ,'6408d55611798f10f203fe5b' AS providedby_id --DataPlat
      ,SUM(unblended_cost + reservation_cost) AS total_cost
  FROM
      {{ ref('aws_costs_usage') }}
      --"custos_cloud_silver"."aws_costs_usage"
  WHERE
      aws_product_name = 'Amazon Athena'
  --GROUP BY 1,2,3,4,5
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
-- workgroups compartilhados 
shared_workgroups (workgroup) AS (
  VALUES ('primary')
        ,('dbt_workgroup')
        ,('looker_workgroup')
),
-- utilizacao de workgroups compartilhados, rateados por circulo
shared_workgroup_usage AS(       
    SELECT DISTINCT
        DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)) AS start_date
        ,id_account AS account_id
        ,ds_workgroup AS workgroup
        ,id_circulo AS circle_id
        --,SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup) AS total_workgroup_datascannedinbytes
        --,SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_circulo) AS circle_workgroup_datascannedinbytes
        --,CASE
        --  WHEN SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup) = 0
        --    THEN 0.0
        --  ELSE
        --    CAST(SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_circulo) AS DOUBLE) /
        --    CAST(SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup) AS DOUBLE)
        --  END AS usage_percentage
        ,SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_account) AS total_workgroup_datascannedinbytes
        ,SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_account, id_circulo) AS circle_workgroup_datascannedinbytes
        ,CASE
          WHEN SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_account) = 0
            THEN 0.0
          ELSE
            CAST(SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_account, id_circulo) AS DOUBLE) /
            CAST(SUM(nr_datascannedinbytes) OVER(PARTITION BY DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)), ds_workgroup, id_account) AS DOUBLE)
          END AS usage_percentage
    FROM
        {{ source('datametrics_gold', 'metricas_circulos_dia') }}
        --"datametrics_gold"."metricas_circulos_dia"
    WHERE
        nm_engine='athena'
        AND ds_workgroup IN (SELECT workgroup from shared_workgroups)
        --AND ds_workgroup IN ('primary','dbt_workgroup','looker_workgroup')
),
-- custos de workgroups compartilhados, rateados por circulo
shared_workgroups_costs AS
(
  SELECT COALESCE(a.start_date,w.start_date) as start_date
        -- corrigir após correcao datametrics
        ,a.account_id AS account_id
        ,a.environment AS environment
        ,a.account_name AS account_name
        ,a.region AS region
        ,a.billing_origin
        ,a.workgroup
        ,a.aws_product_name
        ,COALESCE(w.circle_id,'6408d55611798f10f203fe5b') circle_id --fallback DataPlat, custos de Dev
        ,a.providedby_id
        ,COALESCE(a.total_cost * w.usage_percentage, a.total_cost) as total_cost --caso não dê match no join com a usage
        ,w.circle_workgroup_datascannedinbytes as nr_datascannedinbytes
  FROM athena_costs a
  LEFT JOIN shared_workgroup_usage w
    ON a.start_date = w.start_date
    AND a.account_id = w.account_id
   AND a.workgroup = w.workgroup
  WHERE a.workgroup IN (SELECT workgroup from shared_workgroups)
  --WHERE workgroup IN ('primary','dbt_workgroup','looker_workgroup')
),

-- utilizacao de workgroups dedicados, alocados diretamente nos circulos donos dos workgroups
dedicated_workgroup_usage AS(       
    SELECT
        DATE_TRUNC('day', CAST(dt_submission AS TIMESTAMP)) AS start_date
        ,id_account AS account_id
        ,ds_workgroup AS workgroup
        ,SUM(nr_datascannedinbytes) AS total_workgroup_datascannedinbytes
    FROM
        {{ source('datametrics_gold', 'metricas_circulos_dia') }}
        --"datametrics_gold"."metricas_circulos_dia"
    WHERE
        nm_engine='athena'
        AND ds_workgroup NOT IN (SELECT workgroup from shared_workgroups)
        --AND ds_workgroup NOT IN ('primary','dbt_workgroup','looker_workgroup')
    --GROUP BY 1,2
    GROUP BY 1,2,3
),

-- custos de workgroups dedicados, alocados diretamente nos circulos donos dos workgroups
dedicated_workgroups_costs AS
(
  SELECT a.start_date
        ,a.account_id
        ,a.environment
        ,a.account_name
        ,a.region
        ,a.billing_origin
        ,a.workgroup
        ,a.aws_product_name
        ,a.circle_id -- mantém o circle_id da tag da aws
        ,a.providedby_id
        ,a.total_cost
        ,w.total_workgroup_datascannedinbytes as nr_datascannedinbytes
  FROM athena_costs a
  LEFT JOIN dedicated_workgroup_usage w
    ON a.start_date = w.start_date
   AND a.account_id = w.account_id
   AND a.workgroup = w.workgroup
  WHERE a.workgroup NOT IN (SELECT workgroup from shared_workgroups)
  --WHERE a.workgroup NOT IN ('primary','dbt_workgroup','looker_workgroup')
)

SELECT d.start_date
        --,'142401413602' AS account_id
        --,'production' AS environment
        --,'will-production' AS account_name
        --,'sa-east-1' AS region
        --,'AWS' AS billing_origin
        ,d.account_id
        ,d.environment
        ,d.account_name
        ,d.region
        ,d.billing_origin
        ,d.workgroup
        ,d.aws_product_name
        ,d.circle_id
        ,d.providedby_id
        ,d.total_cost
        ,d.nr_datascannedinbytes
FROM dedicated_workgroups_costs d

UNION ALL

SELECT s.start_date
        --,'142401413602' AS account_id
        --,'production' AS environment
        --,'will-production' AS account_name
        --,'sa-east-1' AS region
        --,'AWS' AS billing_origin
        ,s.account_id
        ,s.environment
        ,s.account_name
        ,s.region
        ,s.billing_origin
        ,s.workgroup
        ,s.aws_product_name
        ,s.circle_id
        ,s.providedby_id
        ,s.total_cost
        ,s.nr_datascannedinbytes
FROM shared_workgroups_costs s
