-- TABELA PARA IDENTIFICAR CONTAS AWS POR NOME AMIGAVEL
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT E EKS_COST_USAGE_REPORT

WITH aws_account_names(account_id, account_name) AS (
    VALUES
		 ('887592687927','master')
		,('849517169598','analytics')
		,('684942206573','avista')
		,('607032490329','cfi-dev')
		,('756377333309','cfi-prod')
		,('696531655624','invista')
		,('905873187999','p20-dev')
		,('648457633454','p20-prod')
		,('937029184075','p20-stg')
		,('739007973549','pag-production')
		,('202426948818','pag-sandbox')
		,('876897421480','will-development')
		,('142401413602','will-production')
		,('607150308374','will-redes')
		,('786189240756','will-security')
		,('287431677215','audit')
		,('470606486813','log-archive')
		,('669838040723','will-redteam')
		,('214641885395','data-platatfom-development')
		,('533267259976','data-platatfom-production')
		,('851725448065','will-soc-dfir')
		,('381492161506','plateng-sandbox')
		,('343218220321','will-grc-platform')
)

SELECT account_id, account_name FROM aws_account_names
