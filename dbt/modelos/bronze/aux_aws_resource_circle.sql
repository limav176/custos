-- TABELA AUXILIAR DIRECIONAR CUSTOS DE RECURSOS MAIS CAROS NA AWS A UM CIRCULO
-- UTILIZADO NA TABELA: STG_AWS_COST_USAGE_REPORT

WITH
resources_deleted(resource_id, circle_id) AS (
    VALUES
        ('arn:aws:athena:sa-east-1:142401413602:capacity-reservation/data-platform-dags-104-dpu-cluster','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:sagemaker:sa-east-1:142401413602:app/d-septvsj9odxs/carlos-quixada-willbank-com-br-5f5/kernelgateway/sagemaker-data-scie-ml-m5-24xlarge-e1083cbba5bf2c874d02e77f2371','6388cfe9f4639029ca046234') --Plataforma de MLOps
        ,('arn:aws:ec2:sa-east-1:786189240756:natgateway/nat-0a8e23067a2a37668','6221314e8fac213d65503e08') --Cyber Security
        ,('pacman-data-analytics-stream-prod','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:sagemaker:sa-east-1:142401413602:endpoint/facematch-sdx','629a10805f31de1ea872a9f9') --Prevenção à fraude
        ,('arn:aws:logs:sa-east-1:887592687927:log-group:aws-controltower/CloudTrailLogs','6221314e8fac213d65503e08') --Cyber Security
        ,('arn:aws:rds:us-east-1:756377333309:db:prod-db','622130e9ed27e66884250012') --Conta
        ,('arn:aws:dms:sa-east-1:142401413602:rep:ALFZJCJ4BVHR5GKA2PMCGW2CAY','63066a1465ceb52969778c5a') --Financeiro
        ,('arn:aws:kinesis:sa-east-1:142401413602:stream/pacman-data-analytics-stream','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:rds:sa-east-1:849517169598:db:credit-engine-rw','64af0adb207369f4a1052fa9') --Crédito
        ,('arn:aws:firehose:sa-east-1:142401413602:deliverystream/pacman-firehose-to-s3-with-parquet','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:rds:sa-east-1:142401413602:db:rds-pdd-cluster-instance-0','632c790392dbd13f6b5484f4') --Recuperação
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-core-invoice-installment-payment-prod','62212eb60589bb79515a3995') --Cartões
        ,('arn:aws:sagemaker:sa-east-1:142401413602:endpoint/application-game-banc-prod','64af0adb207369f4a1052fa9') --Crédito
        ,('arn:aws:sagemaker:sa-east-1:142401413602:endpoint/application-game-nbanc-prod','64af0adb207369f4a1052fa9') --Crédito
        ,('arn:aws:sqs:sa-east-1:142401413602:crm-callback-log-salesforce','6299348e231a6611ae55f82f') --CXM
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-core-customer-prod','6299348e231a6611ae55f82f') --CXM
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-core-customer-prod-ro-0','6299348e231a6611ae55f82f') --CXM
        ,('arn:aws:rds:sa-east-1:142401413602:db:card-cluster-instance-0','62212eb60589bb79515a3995') --Cartões
        ,('arn:aws:athena:sa-east-1:142401413602:capacity-reservation/capacity_reservation','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:lambda:sa-east-1:142401413602:function:credit-events','64af0adb207369f4a1052fa9') --Crédito
        ,('data-app-results-zone-pag-prod','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:glue:sa-east-1:142401413602:crawler/raw_zone_pacman','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-logistics-prod','62212eb60589bb79515a3995') --Cartões
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-access-control-prod','629a10805f31de1ea872a9f9') --Prevenção à fraude
        ,('will-prod-meuwill','6299348e231a6611ae55f82f') --CXM
        ,('arn:aws:rds:sa-east-1:142401413602:db:pix-platform-cluster-instance-0','622130e9ed27e66884250012') --Conta
        ,('arn:aws:athena:sa-east-1:142401413602:capacity-reservation/temporary_capacity_reservation','6408d55611798f10f203fe5b') --Data Platform
        ,('will-prod-cdp-limit-maintenance-varbook','64af0adb207369f4a1052fa9') --Crédito
        ,('will-prod-ml-platform-sagemaker-studio','6388cfe9f4639029ca046234') --Plataforma de MLOps
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-core-card-prod','62212eb60589bb79515a3995') --Cartões
        ,('will-invoice-pdfs-prod','62212eb60589bb79515a3995') --Cartões
        ,('arn:aws:sagemaker:sa-east-1:142401413602:endpoint/application-score-nbanc-prod','64af0adb207369f4a1052fa9') --Crédito
        ,('arn:aws:sagemaker:sa-east-1:142401413602:endpoint/application-score-banc-prod','64af0adb207369f4a1052fa9') --Crédito
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-notification-service','65a196d5a55f7b068f05df9c') --Infra Reliability
        ,('arn:aws:osis:sa-east-1:142401413602:pipeline/pacman-frontend-prod','6408d55611798f10f203fe5b') --Data Platform
        ,('cdp-preapproved-policy-prod','64af0adb207369f4a1052fa9') --Crédito
        ,('arn:aws:dynamodb:sa-east-1:142401413602:table/user_login_history','629a10805f31de1ea872a9f9') --Prevenção à fraude
        ,('arn:aws:sagemaker:sa-east-1:142401413602:endpoint/fraudev3-endpoint-prod','629a10805f31de1ea872a9f9') --Prevenção à fraude
        ,('backup-financ-prod','63066a1465ceb52969778c5a') --Financeiro
        ,('arn:aws:athena:sa-east-1:142401413602:capacity-reservation/data-platform-dags-156-dpu-cluster','6408d55611798f10f203fe5b') --Data Platform
        ,('i-061ae31aa562a5fad','65a196d5a55f7b068f05df9c') --Infra Reliability
        ,('arn:aws:rds:us-east-1:756377333309:db:servicecfi','622130e9ed27e66884250012') --Conta
        ,('i-0fb25c89357f5ffeb','65a196d5a55f7b068f05df9c') --Infra Reliability
        ,('arn:aws:rds:sa-east-1:142401413602:db:postgresql-core-challenge-prod','629a10805f31de1ea872a9f9') --Prevenção à fraude
        ,('arn:aws:rds:sa-east-1:142401413602:db:will-datalake-database','6408d55611798f10f203fe5b') --Data Platform
        ,('arn:aws:sagemaker:sa-east-1:142401413602:app/d-septvsj9odxs/carlos-quixada-willbank-com-br-5f5/kernelgateway/sagemaker-data-scien-ml-m5-8xlarge-5c6fb27aee407e175ebd3b987c5d','6388cfe9f4639029ca046234') --Plataforma de MLOps
        ,('arn:aws:sagemaker:sa-east-1:142401413602:app/d-o2ivyxiauiso/gabriel-bastos-willbank-com-br-b23/kernelgateway/sagemaker-data-scien-ml-r5-4xlarge-0b3ca327429912db5671c0529617','6388cfe9f4639029ca046234') --Plataforma de MLOps
        ,('arn:aws:osis:us-east-1:876897421480:pipeline/pacman-frontend-dev','6408d55611798f10f203fe5b') --Data Platform
        ,('i-079bce556e6b2d8c4','6221314e8fac213d65503e08') --Cyber Security
        ,('i-0d78f0b44224880d7','63066a1465ceb52969778c5a') --Financeiro
        ,('arn:aws:rds:sa-east-1:142401413602:cluster:cluster-eibvl4bnxozyql6b7uo5r4is2i','632c790392dbd13f6b5484f4') --Recuperação
        ,('i-0f4bfecc2ae5693f2','6299348e231a6611ae55f82f') --CXM
        ,('i-049358dd7a335babc','6299348e231a6611ae55f82f') --CXM
        ,('arn:aws:elasticloadbalancing:sa-east-1:142401413602:loadbalancer/app/k8s-pong-pong-5ff63221b1/c33357dbe57113e4','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('arn:aws:elasticloadbalancing:sa-east-1:142401413602:loadbalancer/app/82287b90-bffapp-bffappingr-834c/ff3123c77ea9ff50','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('arn:aws:elasticloadbalancing:sa-east-1:142401413602:loadbalancer/net/a3c738e06466b4148bf95cd871e65a27/dd6763f75fa7a51e','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('arn:aws:elasticloadbalancing:sa-east-1:142401413602:loadbalancer/net/a2da8e74a47304b3c82506e49f3e5635/0d0aa73bee2df973','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('arn:aws:elasticloadbalancing:sa-east-1:142401413602:loadbalancer/net/a37eff67272554cb9818b0ae30dd58ba/428ea81c109b0913','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('arn:aws:elasticloadbalancing:sa-east-1:142401413602:loadbalancer/net/a5e183ac48aaf4b18b7dbe83a5cf7eb7/da29277943a16fb8','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('vol-06f4d6d6901ec1155','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('vol-074e4821e1e2127ea','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('vol-00a445cdadc232c50','65a196d5a55f7b068f05df9c') -- Infra Reliability
        ,('vol-044b000c5f3b5ebb7','6350774369923deed60fb21c') -- Aquisição (Cartões)
),
resources_untaggable(resource_id, circle_id) AS (
    VALUES
        -- Cyber Security
        ('arn:aws:kms:us-east-2:786189240756:key/fef32fd7-345b-444a-8a77-f23121f62c40', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-2:786189240756:key/35a0f400-daa3-4bea-8108-c2c07051a0a8', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:baseline-overrides-b8be-0azsx:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/3b0b4954-11c4-4078-a50e-cc4c3c2c15fa', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:start-stop-dev-function:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:baseline-overrides-b8b3-ccjws:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:instanceScheduler-pag-sandbox-InstanceSchedulerMain:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:list-iam-users:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:notificao-erro:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:turn-off-pag-environment:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:send-push-sandbox:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:cs-horizon-sensor-installation-orchestrator:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:start-batch-oracle-sandbox:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:202426948818:function:aws-controltower-NotificationForwarder:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/77e7be8a-65b9-46e3-9435-fba485830b5b', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/a79a9624-5d4f-4ccb-a326-94e6479a6532', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:sa-east-1:287431677215:aws-controltower-AllConfigNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/b0de1591-643d-4170-9bd4-bbf7e166db2b', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:sa-east-1:287431677215:aws-controltower-AggregateSecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-1:287431677215:aws-controltower-AggregateSecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-1:287431677215:aws-controltower-AllConfigNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/c9653037-0254-4da4-944b-26711e33fb8a', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/fcf33126-73cf-41a8-b82b-4e220b227248', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/be8c97fe-dd18-4558-9ae0-78e3884e398b', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/f9640662-7af0-4949-bc85-9a48c19f4665', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/f6ec2be2-22dd-4ef3-81b7-98bbb135816e', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/77d6d844-d75c-4036-bbf0-d71cce1ce98d', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/de53c880-3bcd-49ef-9d4c-d4f749ffc7a8', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/a3c2ac29-d111-4be4-a17d-e9de4dee2dc9', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/373126f7-19c8-4a4a-ad80-7a81df0cbc42', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/e473338e-83c1-4b6f-81e4-860e83203c1b', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/7f7d16eb-3777-4a7b-adee-ddf578ac4cce', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:786189240756:key/68022af7-5b86-44b1-a9ab-b6951081f150', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:786189240756:key/b9750b1b-633b-4e02-8743-9df3388dbbca', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:669838040723:key/77004543-e2b4-40b5-a060-440514d937d5', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:287431677215:key/bef98c09-514b-4903-a53d-6a2a2103d672', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:669838040723:key/af502970-ae22-44be-8595-b0647df69d04', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:287431677215:key/79978252-6f0e-433e-acdd-fb32e881125a', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:287431677215:key/802f240c-71cc-4c65-8e4b-c0b79f1be38d', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:470606486813:key/5ec1e078-1509-4936-91e6-cc1fd117b974', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:669838040723:key/bdb0fddc-37f8-4426-b9ea-0c1235b28f15', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:287431677215:key/6d41b8a1-b800-4407-b31d-0e73182de46b', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:470606486813:key/b3f5d2e0-0f0c-4960-bc94-4384c6f34591', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:470606486813:key/b78ff034-1959-4f31-b34f-b7a0c7301517', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:470606486813:key/e9a3c95f-e50b-447c-b7db-720622826fe3', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:669838040723:key/b3d11af9-33cc-4732-bc86-ea19b3154f59', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:470606486813:key/9dd44f9a-73a9-4085-899e-ab4946bcaddd', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:669838040723:key/3c84ff8c-686b-4fab-a062-4cc12dcd0071', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:470606486813:key/a6372099-410c-4644-ba01-7ace3783037c', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:287431677215:key/86de0ef6-7a54-4602-9f7e-01612482398c', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:287431677215:key/5eae4542-5683-497b-824a-e16ae8711432', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:669838040723:key/b6850232-0165-4ab1-b252-30bf1a426d4c', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:287431677215:key/83235c3b-0914-49d2-add9-2ef2e8f34460', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-west-2:470606486813:key/f2f51b41-93d3-4a49-a93e-5a45e1a8203c', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-2:470606486813:key/c9c42926-69f1-40b8-9ee5-fa0b1c7f5875', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:851725448065:key/b3fce255-1c8a-4e66-a879-dfc47c7ecbf2', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-west-2:287431677215:key/6fe72dbb-8528-483e-b430-984da2802bc6', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-2:287431677215:key/963d5f45-bcd2-428c-b291-0509ffff458c', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-2:669838040723:key/120689bd-183b-4af7-9e45-c635d7872ba1', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:851725448065:key/2da18aea-8cc0-4e93-be4e-5d0c3c0044eb', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-west-2:669838040723:key/0e03d973-3192-4eba-a9d7-a8ecbf1f07d3', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:669838040723:function:aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:851725448065:key/b795a30e-fa30-414e-aa79-dfcfdab84006', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-west-2:851725448065:key/bffad0fc-cb37-4af4-8813-38b678f7bc18', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-west-2:786189240756:key/71bb9d17-c494-4edb-bcca-9c3a82fa271c', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-2:786189240756:key/c479fdff-df57-401f-90ed-20ab361551af', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-2:851725448065:key/992b72e3-162f-4c4b-8ed7-2df3efa38b19', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:669838040723:function:aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-west-2:786189240756:key/6b6f8acb-7416-4d89-b339-4fbdc78eb9cc', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:sa-east-1:287431677215:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:sa-east-1:851725448065:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:sa-east-1:470606486813:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-1:287431677215:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-1:851725448065:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-1:470606486813:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:470606486813:function:aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:470606486813:function:aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:us-east-1:470606486813:log-group:/aws/lambda/aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:287431677215:function:aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:sa-east-1:287431677215:log-group:/aws/lambda/aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:sa-east-1:470606486813:log-group:/aws/lambda/aws-controltower-NotificationForwarder', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:786189240756:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:470606486813:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:851725448065:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:287431677215:aws-controltower-AggregateSecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:786189240756:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:669838040723:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:287431677215:aws-controltower-AllConfigNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:macie2:sa-east-1:786189240756:classification-job/afdcbaa9541132eb3755a58bc4ba8085', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:669838040723:key/79c670f9-fa0f-4f5b-b766-dc41a06cfe79', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:851725448065:key/0b271455-e236-4fd8-8260-ded17070d6ca', '62993c119a69c7228362fceb')
        ,('arn:aws:securityhub:sa-east-1:607032490329:hub/default', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:607150308374:function:aws-fgt-backup:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:607150308374:function:aws-controltower-NotificationForwarder:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:607150308374:function:baseline-overrides-90dd-xmj4b:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:607150308374:function:fgt-bkp-randompass:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:756377333309:function:ted_aguardando_resposta:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:756377333309:function:aws-controltower-NotificationForwarder:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:us-east-1:756377333309:function:slack-notification-api-gateway-5xx:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:secretsmanager:us-east-1:142401413602:secret:core-payment-rest-69Y87e', '62993c119a69c7228362fceb')
        ,('arn:aws:secretsmanager:us-east-1:142401413602:secret:secret_refresh_notification_token_key_prod-JBO3bK', '62993c119a69c7228362fceb')
        ,('arn:aws:secretsmanager:us-east-1:142401413602:secret:sync-incidentio-pulse-secret-IShiuT', '62993c119a69c7228362fceb')
        ,('arn:aws:secretsmanager:us-east-1:142401413602:secret:secret_unleash_core_card_key_dev-5B217A', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:851725448065:key/d52eba32-7d0e-4431-a6b2-0da604bf8cef', '62993c119a69c7228362fceb')
        ,('arn:aws:secretsmanager:us-east-1:905873187999:secret:dev/core-ted-api-websocket-pLQREY', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:669838040723:key/45458371-c36d-494b-9572-68b744988bdc', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:sa-east-1:669838040723:key/b3c5fa7a-8b80-41e2-a95b-5e52cf3ab45e', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:669838040723:key/fc5d969b-60e2-430c-bb31-f34f7c9bf3e2', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:470606486813:key/837d3005-1a21-48e8-b059-f01b69ff4b5d', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:287431677215:key/7a745f99-3ce4-40d5-81ed-2b32e5e343f1', '62993c119a69c7228362fceb')
        ,('arn:aws:kms:us-east-1:851725448065:key/7808f628-a058-43d1-8e03-82cf6b7ba978', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:sa-east-1:786189240756:log-group:/aws/guardduty/malware-scan-events', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:us-east-1:786189240756:log-group:/aws/guardduty/malware-scan-events', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:us-east-1:786189240756:log-group:/aws/lambda/delete-name-tags-us-east-1-ba4d-b4db2', '62993c119a69c7228362fceb')
        ,('arn:aws:logs:us-west-2:786189240756:log-group:/aws/lambda/Nops-Integration-10b6-NopsLambdaLookupStack-oRsW0WUDflXv', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:287431677215:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:287431677215:aws-controltower-AllConfigNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:lambda:sa-east-1:202426948818:function:aws-controltower-NotificationForwarder:$LATEST', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:851725448065:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:287431677215:aws-controltower-AggregateSecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:287431677215:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-west-2:669838040723:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
        ,('arn:aws:sns:us-east-2:470606486813:aws-controltower-SecurityNotifications', '62993c119a69c7228362fceb')
),

final AS (
SELECT resource_id, circle_id FROM resources_deleted
                UNION ALL
SELECT resource_id, circle_id FROM resources_untaggable
)

SELECT resource_id, circle_id FROM final
