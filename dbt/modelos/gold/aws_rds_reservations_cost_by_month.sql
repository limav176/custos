WITH source AS ( SELECT * FROM {{ ref('silver_aws_rds_reservations_costs') }} )
SELECT * FROM source
