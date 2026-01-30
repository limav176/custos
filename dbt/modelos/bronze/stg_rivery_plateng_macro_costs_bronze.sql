WITH source AS (
    SELECT * FROM {{ source('custos_cloud_bronze', 'rivery_plateng_macro_costs_bronze') }}
)

SELECT 
    team
    ,domain
    ,action
    ,date
    ,product
    ,cost
    ,saving_opportunity
    ,saving
    ,status
    ,card
    ,cardlink
FROM source
GROUP BY 11,10,9,8,7,6,5,4,3,2,1
