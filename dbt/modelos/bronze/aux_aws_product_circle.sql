-- TABELA AUXILIAR DIRECIONAR CUSTOS DE DETERMINADOS PRODUTOS AWS A UM CIRCULO
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT

WITH aux_aws_product_circle(circle_id, product_name) AS (
    VALUES
		 ('62993c119a69c7228362fceb', 'Amazon GuardDuty') -- CloudSec
		,('6221314e8fac213d65503e08', 'AWS Config') -- Cyber Security
		,('62993c119a69c7228362fceb', 'AWS Security Hub') -- CloudSec
		,('62993c119a69c7228362fceb', 'Amazon Inspector') -- CloudSec
		,('6221314e8fac213d65503e08', 'AWS CloudTrail') -- Cyber Security
		,('62993c119a69c7228362fceb', 'AWS Key Management Service') -- CloudSec
		,('62993c119a69c7228362fceb', 'AWS Systems Manager') -- CloudSec
		-- AWS Marketplace
		,('62992fc2c997c81650231188', 'OCBAWS Brazil 2P') -- Datadog (Observability)
		,('62992fc2c997c81650231188', 'Datadog') -- Observability
		,('62992fc2c997c81650231188', 'Datadog Enterprise') -- Observability
		,('6221314e8fac213d65503e08', 'Fortinet FortiGate Next-Generation Firewall') -- Cyber Security
		,('62992fc2c997c81650231188', 'Embrace') -- Observability
		,('6221314e8fac213d65503e08', 'CloudBeaver AWS') -- Cyber Security
		,('6221314e8fac213d65503e08', 'Fortinet Managed Rules for AWS WAF Classic - Complete OWASP Top 10') -- Cyber Security
)

SELECT circle_id, product_name FROM aux_aws_product_circle
