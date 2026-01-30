-- TABELA AUXILIAR DIRECIONAR CUSTOS DE DETERMINADOS PRODUTOS AWS A UM CIRCULO
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT

WITH aux_aws_tag_name_circle(circle_id, tag_name) AS (
    VALUES
		-- Infra Reliability (EKS) - WILL-PROD-SHARED
		('65a196d5a55f7b068f05df9c', 'will-prod-shared-jobs')
		,('65a196d5a55f7b068f05df9c', 'will-prod-shared-karpenter')
		,('65a196d5a55f7b068f05df9c', 'will-prod-shared')
		,('65a196d5a55f7b068f05df9c', 'will-prod-shared-0')
		,('65a196d5a55f7b068f05df9c', 'will-prod-shared-1')
		,('65a196d5a55f7b068f05df9c', 'will-prod-shared-2')
		,('65a196d5a55f7b068f05df9c', 'will-prod-shared-3')
		-- Infra Reliability (EKS) - WILL-DEV-SHARED
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared-jobs')
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared-karpenter')
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared')
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared-0')
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared-1')
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared-2')
		,('65a196d5a55f7b068f05df9c', 'will-dev-shared-3')
		-- Infra Reliability (EKS) - WILL-PROD-FOUNDATION-PLATFORMS
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms-jobs')
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms-karpenter')
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms')
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms-0')
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms-1')
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms-2')
		,('65a196d5a55f7b068f05df9c', 'will-prod-foundation-platforms-3')
		-- Infra Reliability (EKS) - WILL-DEV-FOUNDATION-PLATFORMS
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms-jobs')
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms-karpenter')
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms')
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms-0')
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms-1')
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms-2')
		,('65a196d5a55f7b068f05df9c', 'will-dev-foundation-platforms-3')
		-- Infra Reliability (EKS) - LEGACY
		,('65a196d5a55f7b068f05df9c', 'eks-will-prod-01')
		,('65a196d5a55f7b068f05df9c', 'eks-will-dev-02')
)

SELECT circle_id, tag_name FROM aux_aws_tag_name_circle
