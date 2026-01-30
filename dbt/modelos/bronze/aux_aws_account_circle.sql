-- TABELA AUXILIAR PARA DIRECIONAR CUSTOS DE DETERMINADAS CONTAS AWS PARA UM CIRCULO
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT E EKS_COST_USAGE_REPORT

WITH aux_aws_account_circle(account_id, circle_id) AS (
    VALUES
		 ('849517169598','6221311e93a3d46920739acd') -- 'analytics', Crédito 64af0adb207369f4a1052fa9
		,('607032490329','622130e9ed27e66884250012') -- 'cfi-dev', Conta
		,('756377333309','622130e9ed27e66884250012') -- 'cfi-prod', Conta
		,('696531655624','67a4cb37d68f208f4b05ef32') -- 'invista', Savings - Conta
		,('607150308374','65a196d5a55f7b068f05df9c') -- 'will-redes', Infra Reliability - Platform-Engineering
		,('786189240756','6221314e8fac213d65503e08') -- 'will-security', Cyber Security
		,('287431677215','6221314e8fac213d65503e08') -- 'audit', Cyber Security
		,('470606486813','6221314e8fac213d65503e08') -- 'log-archive', Cyber Security
		,('669838040723','6221314e8fac213d65503e08') -- 'will-redteam', Cyber Security
		,('214641885395','6408d55611798f10f203fe5b') -- 'data-platatfom-development', Dados
		,('533267259976','6408d55611798f10f203fe5b') -- 'data_platatfom_production', Dados
		,('851725448065','6221314e8fac213d65503e08') -- 'will-soc-dfir', Cyber Security
		,('381492161506','6221311e93a3d46920739acd') -- 'plateng-sandbox', Platform-Engineering
)

SELECT account_id, circle_id FROM aux_aws_account_circle
