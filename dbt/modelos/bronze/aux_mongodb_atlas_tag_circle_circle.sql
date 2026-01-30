-- TABELA AUXILIAR PARA CORRELACIONAR TAG JÁ EXISTENTE (CIRCLE) A UM CIRCULO
-- UTILIZADO NA TABELA: STG_MONGODB_ATLAS_COST_USAGE_REPORT

WITH aux_mongodb_atlas_tag_circle_circle(circle_id, tag_circle) AS (
    VALUES
        ('622130e9ed27e66884250012','Conta')
        ,('629a10805f31de1ea872a9f9','Fraude')
        ,('6221311e93a3d46920739acd','Shared')
        ,('63066a1465ceb52969778c5a','Finance')
        ,('6299348e231a6611ae55f82f','CXM')
        ,('628ac9889da45c63807f6bed','Marketplace')
        ,('64af0adb207369f4a1052fa9','Credit')
        ,('62992ff495dd587791322b49','Mobile Platform')
        ,('62212eb60589bb79515a3995','Cartoes')
        ,('6350774369923deed60fb21c','Aquisicao')
        ,('632a37a8b802205fcc43375f','MarTech')
)

SELECT circle_id, tag_circle FROM aux_mongodb_atlas_tag_circle_circle
