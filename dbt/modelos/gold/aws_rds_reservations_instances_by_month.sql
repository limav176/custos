WITH source AS ( SELECT * FROM {{ ref('silver_aws_rds_reservations_instances_details') }} )
SELECT * FROM source
