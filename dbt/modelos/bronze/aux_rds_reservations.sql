WITH source(reservation_id, circle_id, reservations, start_time, end_time, engine, multi_az) AS (
    VALUES
    -- Aurora PostgreSQL db.t4g.medium
        ('18895591703','62993a16688fc213901e384c',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','63a0505d645bbf5dd70892ad',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','65f32f0d2494e93c390e668d',4,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','63c1d0f4dfeae762f901da72',3,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','67a4cb4bd68f208f4b05ef63',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','6388cfca066715d67b0c8a03',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','63a0505d645bbf5dd70892ad',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','65f882694c92d1ebb20bcf73',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','632a320a2a18003e7415ed6c',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','62993a16688fc213901e384c',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','63724e228b22de6c2609ddd3',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','6350774369923deed60fb21c',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','6408d55611798f10f203fe5b',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','65f882694c92d1ebb20bcf73',3,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','632a320a2a18003e7415ed6c',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
        ,('18895591703','629a10a40d5af42bf611f0d9',1,'2025-02-25 19:45:22','2026-02-25 19:45:21','aurora-postgresql','Single-AZ')
    -- Aurora PostgreSQL db.r6g.large
        ,('18895577477','6388cfca066715d67b0c8a03',3,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','622130e9ed27e66884250012',2,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','65a196d5a55f7b068f05df9c',1,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','62993a16688fc213901e384c',3,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','63a0505d645bbf5dd70892ad',3,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','6372520140067a15f40e9b49',3,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','629a10a40d5af42bf611f0d9',1,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','63c1d0f4dfeae762f901da72',2,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
        ,('18895577477','63a051886dcbb65fe10ce571',2,'2025-02-25 19:43:09','2026-02-25 19:43:08','aurora-postgresql','Single-AZ')
    -- Aurora PostgreSQL db.r7g.large
        ,('18895597857','622131a7442734149c77e585',2,'2025-02-25 19:46:32','2026-02-25 19:46:31','aurora-postgresql','Single-AZ')
        ,('18895597857','67a4cb4bd68f208f4b05ef63',2,'2025-02-25 19:46:32','2026-02-25 19:46:31','aurora-postgresql','Single-AZ')
        ,('18895597857','6350774369923deed60fb21c',2,'2025-02-25 19:46:32','2026-02-25 19:46:31','aurora-postgresql','Single-AZ')
        ,('18895597857','6350774369923deed60fb21c',2,'2025-02-25 19:46:32','2026-02-25 19:46:31','aurora-postgresql','Single-AZ')
    -- Aurora PostgreSQL db.t4g.medium
        ,('19221602192','65f32f0d2494e93c390e668d',2,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','628ac9889da45c63807f6bed',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','65f882694c92d1ebb20bcf73',2,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','63a0505d645bbf5dd70892ad',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','62993a16688fc213901e384c',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','63c1d0f4dfeae762f901da72',6,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','67a4cb295e77d096c607068b',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','6388cfca066715d67b0c8a03',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','632cccaea7dce6718900084b',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','62993c25843117207a09917f',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
        ,('19221602192','6408d55611798f10f203fe5b',1,'2025-03-20 13:49:55','2026-03-20 13:49:54','aurora-postgresql','Single-AZ')
    -- Aurora PostgreSQL db.r6g.large
        ,('19224981519','62993a16688fc213901e384c',2,'2025-03-20 18:11:09','2026-03-20 18:11:08','aurora-postgresql','Single-AZ')
        ,('19224981519','65a196d5a55f7b068f05df9c',1,'2025-03-20 18:11:09','2026-03-20 18:11:08','aurora-postgresql','Single-AZ')
        ,('19224981519','6388cfca066715d67b0c8a03',1,'2025-03-20 18:11:09','2026-03-20 18:11:08','aurora-postgresql','Single-AZ')
        ,('19224981519','67a4cb295e77d096c607068b',1,'2025-03-20 18:11:09','2026-03-20 18:11:08','aurora-postgresql','Single-AZ')
        ,('19224981519','65f32f0d2494e93c390e668d',1,'2025-03-20 18:11:09','2026-03-20 18:11:08','aurora-postgresql','Single-AZ')
    -- Aurora PostgreSQL db.r7g.large
        ,('19224996884','67a4cb4bd68f208f4b05ef63',1,'2025-03-20 18:12:21','2026-03-20 18:12:20','aurora-postgresql','Single-AZ')
        ,('19224996884','6408d55611798f10f203fe5b',1,'2025-03-20 18:12:21','2026-03-20 18:12:20','aurora-postgresql','Single-AZ')
        ,('19224996884','6350774369923deed60fb21c',1,'2025-03-20 18:12:21','2026-03-20 18:12:20','aurora-postgresql','Single-AZ')
        ,('19224996884','62213c94ee9dfc35c954ae66',1,'2025-03-20 18:12:21','2026-03-20 18:12:20','aurora-postgresql','Single-AZ')
),

staging AS (
    SELECT 
        reservation_id
        ,circle_id
        ,reservations
        ,start_time 
        ,end_time
        ,engine
        ,multi_az
    FROM source
    GROUP BY 7,6,5,4,3,2,1
),

final AS (
SELECT
    CAST(reservation_id AS BIGINT) AS reservation_id
    ,circle_id
    ,CAST(reservations AS INT) AS reservations
    ,CAST(start_time AS TIMESTAMP) AS reservation_start_time
    ,CAST(end_time AS TIMESTAMP) AS reservation_end_time
    ,CASE WHEN engine = 'postgres' THEN 'postgresql'
         WHEN engine = 'oracle-se2' THEN 'oracle'
         ELSE LOWER(REPLACE(engine, ' ', '-'))
    END AS engine
    ,LOWER(REPLACE(multi_az, ' ', '-')) AS multi_az
FROM staging
)

SELECT 
    reservation_id
    ,circle_id
    ,reservations
    ,reservation_start_time
    ,reservation_end_time
    ,engine
    ,multi_az
FROM final
GROUP BY 7,6,5,4,3,2,1