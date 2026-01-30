with aws as(
    select start_date,
        resource_id,
        '' as namespace,
        '' as cluster_name,
    	aws_product_name,
    	circle_id,
    	providedby_id,
        tag_repository,
        tag_product,
    	tag_management,
    	tag_name,
    	environment,
    	usage_type,
        pricing_term,
    	account_id,
    	account_name,
    	region,
        actual_cost,
        baseline_cost
    from {{ ref('reducoes_h1_2025_aws') }}
),
eks as (
    select start_date,
        '' as resource_id,
        namespace,
        cluster_name,
    	'' as aws_product_name,
    	circle_id,
    	'' as providedby_id,
        '' as tag_repository,
        '' as tag_product,
    	'' as tag_management,
    	'' as tag_name,
    	environment,
    	'' as usage_type,
        '' as pricing_term,
    	account_id,
    	account_name,
    	region,
        actual_cost,
        baseline_cost
    from {{ ref('reducoes_h1_2025_eks') }}
),
final as (
    select * from aws
    union all
    select * from eks
)

select *,
case when tag_name in ('vpce-sns', 'vpce-sts', 'vpce-datadog-traces','vpce-datadog-orchestrator','vpce-datadog-process',
    'vpce-datadog-metrics','vpce-datadog-logs-agent','vpce-datadog-api','vpce-datadog-logs-user',
    'main-vpc-prod-nat','main-vpc-dev-nat')
    then 'Criação de VPC Endpoints'
when tag_name in ('airbyte-ingestion-instance-0','airflow-ingestion-instance-0','datalake-instance-0',
    'airbyte-ingestion-dev-instance-0','airflow-ingestion-dev-instance-0')
    then 'Remoção de RDS ociosos de Data Infra'
when tag_name in ('will-prod-backstage-cluster','backstage-cluster-instance-0','will-dev-backstage-cluster') or
    namespace='backstage-app'
    then 'Descomissionamento backstage'
when account_name in ('pag-production','pag-sandbox')
    then  'Reduções ambiente Pag'
when account_name in ('cfi-production','cfi-dev') and
    tag_name like 'launch-config%'
    then  'Revisão instâncias ECS da CFI'
when namespace in ('rtm-db-datadog-proxy','windmill','growthbook','castai-agent','kong-istio-internal',
    'datadog','segmenter','datadog-database-monitoring','istio-system','external-dns','monitoring'
    ,'external-secrets','external-snapshot','reloader','velero','notify','kong-istio-external',
    'karpenter','keda','gha-runner-scale-set-controller','argocd','atlantis','argo-rollouts','platgpt')
    then 'Rightsizing EKS Tech Plat'
when resource_id like '%vpn%'
    then 'Remoção de VPNs desativadas'
when aws_product_name='Amazon Relational Database Service' and
    (usage_type like '%db.r6g.large%' or usage_type like '%db.t4g.medium%' or usage_type like '%db.r7g.large%')
    then 'Reserva de instâncias RDS'
when aws_product_name='AWS Database Migration Service' and
    circle_id='6408d55611798f10f203fe5b'
    then 'Otimização das instâncias permanentes de DMS'
when aws_product_name='Amazon Simple Storage Service' and
    tag_name in ('data-curated-zone-will-prod','data-curated-zone-will-dev','data-bronze-zone-will-prod',
    'data-bronze-zone-will-dev','data-bronze-zone-temp-will-prod')
    then 'Revisar e corrigir, com ajuda dos usuários, os casos de modelos não performáticos'
when aws_product_name='Amazon Simple Storage Service' and
    tag_name like '%raw%'
    then 'Implementar compressão de dados nos buckets de dados Raw'
when aws_product_name='EC2 EMR'
    then 'Otimização da Frota de Instâncias dos Clusters EMR'
when aws_product_name='Amazon ElastiCache' and
    tag_name = 'will-ecache-redis_prod01-prod'
    then 'Redução de nós obsoletos do Redis'
when aws_product_name='Amazon Relational Database Service' and
    tag_name in ('postgresql-prod','postgresql-prod-aurora','credit-engine-cluster')
    then 'Redução no cluster rds postgrelsql-prod'
when account_name = 'will-production' and
    namespace in ('negotiation-policy',
                  'financed-wallet-worker',
                  'credit-transaction-api',
                  'core-credit-card-billing-document-generator',
                  'api-policy-engine')
    then 'SCR Reduções workloads TOP 5'

end as initiative
from final
where start_date >= timestamp '2025-01-01'
