-- TABELA AUXILIAR PARA CORRELACIONAR TAG JÁ EXISTENTE (PRODUCT) A UM CIRCULO
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT

WITH aux_aws_tag_product_circle(circle_id, tag_product) AS (
    VALUES
		 ('6221314e8fac213d65503e08', 'Teleport') -- Cyber Security
		,('62212eb60589bb79515a3995', 'transaction-source-worker') -- Cartões
		,('622130e9ed27e66884250012', 'transferwill') -- Conta
		,('622130e9ed27e66884250012', 'client-portfolio-allocation') -- Conta
		,('62a78ef617382a2bcd429332', 'sisbajud-service') -- Juridico e Compliance
		,('62212eb60589bb79515a3995', 'corecreditcardinvoice') -- Cartões
		,('62993a16688fc213901e384c', 'core-loan') -- Emprestimos
		,('632c790392dbd13f6b5484f4', 'collection-attributes-api') -- Recuperação
		,('62212eb60589bb79515a3995', 'will-fast-movimentacoes-conciliadas') -- Cartões
		,('6408d55611798f10f203fe5b', 'EMR') -- Dados
		,('629a10805f31de1ea872a9f9', 'pldft-kya') -- Prevencao a Fraude
		,('64af0adb207369f4a1052fa9', 'pix-on-credit-api') -- Crédito
		,('62212eb60589bb79515a3995', 'boleto-seeker') -- Cartões
		,('632c790392dbd13f6b5484f4', 'Assessoria') -- Recuperação
		,('6299348e231a6611ae55f82f', 'core-customer-communication-management') -- CXM
		,('62212eb60589bb79515a3995', 'boleto-payment') -- Cartões
		,('62212eb60589bb79515a3995', 'boleto-platform') -- Cartões
		,('62212eb60589bb79515a3995', 'card-data-experiment') -- Cartões
)

SELECT circle_id, tag_product FROM aux_aws_tag_product_circle
