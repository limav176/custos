-- TABELA AUXILIAR DIRECIONAR CUSTOS DE DETERMINADOS SKU DA MONGODB A UM CIRCULO
-- UTILIZADO NA TABELA: STG_MONGODB_ATLAS_COST_USAGE_REPORT

WITH aux_mongodb_atlas_sku_circle(circle_id, sku) AS (
    VALUES
        ('6221311e93a3d46920739acd', 'ATLAS_AWS_PRIVATE_ENDPOINT_CAPACITY_UNITS') -- Platform Engineering
		,('6221311e93a3d46920739acd', 'ATLAS_SUPPORT')                            -- Platform Engineering
		,('6221311e93a3d46920739acd', 'ATLAS_AWS_PRIVATE_ENDPOINT')               -- Platform Engineering
		,('6221311e93a3d46920739acd', 'CHARTS_DATA_DOWNLOADED_FREE_TIER')         -- Platform Engineering
)

SELECT circle_id, sku FROM aux_mongodb_atlas_sku_circle