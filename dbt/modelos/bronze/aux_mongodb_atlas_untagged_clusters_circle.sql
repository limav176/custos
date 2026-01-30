-- TABELA AUXILIAR PARA CORRELACIONAR CLUSTER MONGODB SEM TAG A UM CIRCULO
-- UTILIZADO NA TABELA: STG_MONGODB_ATLAS_COST_USAGE_REPORT

WITH aux_mongodb_atlas_untagged_clusters_circle(circle_id, cluster_name) AS (
    VALUES
        ('65f882694c92d1ebb20bcf73','credit-data-platform')  -- Plataforma de crédito
        ,('622130e9ed27e66884250012','Conta')               -- Conta
        ,('622130e9ed27e66884250012','account')             -- Conta
        ,('632ced9720310d4cb1664438','pcg-varbook-green')   -- Manutenção de Limites
        ,('63a05088b4b34572fe0403a4','growth-cluster-temp') -- Meios de Consumo
        ,('6328d0468846462d97358483','investment')          -- Balance and Savings
        ,('62992ff495dd587791322b49','growthbook')          -- Developer Experience
        ,('62992fc2c997c81650231188','observability')       -- Observability
)

SELECT circle_id, cluster_name FROM aux_mongodb_atlas_untagged_clusters_circle
