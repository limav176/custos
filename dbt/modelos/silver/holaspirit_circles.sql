WITH
custom_circle (holaspirit_id, holaspirit_name, parent_holaspirit_id) AS (
    VALUES
        ('999999999999999999999999', 'Not Tagged', '62212de0578581684e464069')
),

-- A variavel parent_holaspirit_id é o id do parent (caso o circulo tenha sido apagado) ou o id do novo círculo caso tenha sido recriado
dead_circle (holaspirit_id, holaspirit_name, parent_holaspirit_id) AS (
    VALUES
        ('653853e257c143c579059a7b','Data Platform', '6221311e93a3d46920739acd') -- Plataforma de ingestão de Dados
        ,('653854d9d2983d6cb40c0bcb','Data Platform', '6221311e93a3d46920739acd') --Plataforma de processamento e consumo de dados
        ,('6329caa15b623e6d30003a59','Débitos', '622130e9ed27e66884250012') -- Banking transfers movido para Débitos
        ,('6372520140067a15f40e9b49','Credit Services', '62993335c3de167700538e90') -- Credit Services foi recriado e mudou de id
        ,('6328d0468846462d97358483','Balance and Savings','67a4cb295e77d096c607068b') -- Balance and Savings movido para Balances

        --Adicionando todos os id de Finance
        ,('62993335c3de167700538e90','Finance as a Service - FaaS','63066a1465ceb52969778c5a')
        ,('6299336b231a6611ae55f805','Financial Services','63066a1465ceb52969778c5a')
        ,('63724e228b22de6c2609ddd3','Accounting Services','63066a1465ceb52969778c5a')
        ,('637bd4c3d9e17783cb0d423b','ALM','63066a1465ceb52969778c5a')
        ,('632b629547f97674ea3b5dba','Tesouraria','63066a1465ceb52969778c5a')
        ,('66feb15bf7c51c9ac305398e','Tesouraria', '63066a1465ceb52969778c5a')
        ,('65f371be40ccf4df5d0def07','Special Situations','63066a1465ceb52969778c5a')
        ,('65a17b5a3ded4a725b068478','BUC Carteira','63066a1465ceb52969778c5a')
        ,('64ef742246112ff1320d00d7','Business Unit Control (BUC)','63066a1465ceb52969778c5a')
        ,('637bd0d6517a3f191f09e289','Tax','63066a1465ceb52969778c5a')
        ,('65f36e546d644df3630a880f','Cash Management','63066a1465ceb52969778c5a')
        ,('637bcd9d8b797df72f0948bc','Controladoria','63066a1465ceb52969778c5a')
        ,('646bd57d3552bda8880f5a4c','BUC Fechamento','63066a1465ceb52969778c5a')
        ,('64ef74078d6bdcab0409d928','Accounting Product','63066a1465ceb52969778c5a')
        ,('637bd4b4e997caa4ce01e48b','Captação','63066a1465ceb52969778c5a')
        ,('637bd07977314722b10590b8','Tax & Capital Planning','63066a1465ceb52969778c5a')
        ,('63066a1465ceb52969778c5a','Financeiro','62212de0578581684e464069')

        ,('66db44541800c499b20d2855', 'Cartões', '62212eb60589bb79515a3995')
        ,('64af0adb207369f4a1052fa7', 'Crédito', '64af0adb207369f4a1052fa9')
        ,('6719d70765c430f810068eb8', 'Cyber Security', '6221314e8fac213d65503e08')
        ,('62992fc2c997c81650231', 'Observability', '62992fc2c997c81650231188')
        ,('will-prod-pganalyze-snapshots', 'Observability', '62992fc2c997c81650231188')
        ,('teste', 'Observability', '62992fc2c997c81650231188')
        --
        ,('6470a8c59b292016960cac11','Prevenção à fraude','629a10805f31de1ea872a9f9') --Estratégia e Diagnóstico
        ,('629930a4dee9ad5fab709cf7','Operações Backoffice','62212de0578581684e464069') --IT Governance
        ,('65a19729bb405cb82501edd8','Ciência de Dados','672933f22e5b8f32340c4aea') --CD - Corporativo / Visão Cliente
        ,('632b41946bed6d20393636fe','Ciência de Dados','672933f22e5b8f32340c4aea') --Ciência de Dados
        ,('6388c7b46f9145bd9206e9de','Ciência de Dados','672933f22e5b8f32340c4aea') --CD - Produtos de Crédito
        ,('653855429449cb985e07dc18','Hub de Produtos de Dados','6408d55611798f10f203fe5b') -- Data Platform
),

source AS (
    SELECT
        holaspirit_id
        ,holaspirit_name
        ,parent_holaspirit_id
    FROM
        {{ ref('aux_circle_id') }}
    --removendo ids de Finance
    WHERE holaspirit_id NOT IN (
        '62993335c3de167700538e90'
        ,'6299336b231a6611ae55f805'
        ,'63724e228b22de6c2609ddd3'
        ,'63066a1465ceb52969778c5a'
        ,'637bd4c3d9e17783cb0d423b'
        ,'632b629547f97674ea3b5dba'
        ,'66feb15bf7c51c9ac305398e'
        ,'65f371be40ccf4df5d0def07'
        ,'65a17b5a3ded4a725b068478'
        ,'64ef742246112ff1320d00d7'
        ,'637bd0d6517a3f191f09e289'
        ,'65f36e546d644df3630a880f'
        ,'63066a1465ceb52969778c5a'
        ,'637bcd9d8b797df72f0948bc'
        ,'646bd57d3552bda8880f5a4c'
        ,'64ef74078d6bdcab0409d928'
        ,'637bd4b4e997caa4ce01e48b'
        ,'637bd07977314722b10590b8'
        ,'6372520140067a15f40e9b49'
    )
    UNION ALL
    SELECT * FROM custom_circle
    UNION ALL
    SELECT * FROM dead_circle
),

corp_circles (n1_circle_name, corp_name) AS (
    VALUES
     ('IT Governance', 'Corporativo')
    ,('Governance', 'Corporativo')
    ,('Insights', 'Corporativo')
    ,('Jurídico e Compliance', 'Corporativo')
    ,('P&C', 'Corporativo')
    ,('Operações Backoffice', 'Corporativo')
    ,('Riscos', 'Corporativo')
    ,('FP&A e Relações com Investidores', 'Corporativo')

    --overrides
    ,('Customer Experience (CX)', 'CXM')
),

family_tree AS (
    SELECT
        son.holaspirit_id
        ,son.holaspirit_name AS n5
        ,CASE WHEN parent.holaspirit_name = 'will' THEN NULL ELSE parent.holaspirit_name END AS n4
        ,CASE WHEN grandparent.holaspirit_name = 'will' THEN NULL ELSE grandparent.holaspirit_name END AS n3
        ,CASE WHEN ggrandparent.holaspirit_name = 'will' THEN NULL ELSE ggrandparent.holaspirit_name END AS n2
        ,CASE WHEN gggrandparent.holaspirit_name = 'will' THEN NULL ELSE gggrandparent.holaspirit_name END AS n1
        ,CASE WHEN ggggrandparent.holaspirit_name = 'will' THEN NULL ELSE ggggrandparent.holaspirit_name END AS n0
    FROM
        source son
    LEFT JOIN source parent ON son.parent_holaspirit_id = parent.holaspirit_id
    LEFT JOIN source grandparent ON parent.parent_holaspirit_id = grandparent.holaspirit_id
    LEFT JOIN source ggrandparent ON grandparent.parent_holaspirit_id = ggrandparent.holaspirit_id
    LEFT JOIN source gggrandparent ON ggrandparent.parent_holaspirit_id = gggrandparent.holaspirit_id
    LEFT JOIN source ggggrandparent ON gggrandparent.parent_holaspirit_id = ggggrandparent.holaspirit_id
),

tree AS (
    SELECT
        holaspirit_id
        ,COALESCE(n0, n1, n2, n3, n4, n5) AS n1_circle_name
    FROM
        family_tree
),

final AS (
    SELECT
        t.holaspirit_id AS circle_id
        ,h.holaspirit_name AS circle_name
        ,h.parent_holaspirit_id AS parent_circle_id
        ,COALESCE(corp.corp_name, t.n1_circle_name) AS n1_circle_name
    FROM
        tree AS t
    LEFT JOIN source AS h ON h.holaspirit_id = t.holaspirit_id
    LEFT JOIN corp_circles AS corp ON corp.n1_circle_name = t.n1_circle_name
)

SELECT
    *
    ,CASE WHEN
        n1_circle_name IN
            ('Platform Engineering','Not Tagged','Ciência de Dados','Tech Platforms','Product Development Platforms')
        THEN 'platform'
        ELSE 'stream_aligned'
    END AS type
FROM final
