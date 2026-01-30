with dez as(
    select u.start_date,
        u.resource_id,
    	u.aws_product_name,
    	u.circle_id,
    	u.providedby_id,
        u.tag_repository,
        u.tag_product,
    	u.tag_management,
    	u.tag_name,
    	u.environment,
    	u.usage_type,
        u.pricing_term,
    	u.account_id,
    	u.account_name,
    	u.region,
    	sum(u.reservation_cost + u.unblended_cost) as cost
    from {{ ref('aws_costs_day_usage') }} u
    where u.start_date >= timestamp '2024-12-01'
      and u.start_date <= timestamp '2024-12-31'
    group by 15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
avg as (
    select
		resource_id,
    	aws_product_name,
    	circle_id,
    	providedby_id,
        tag_repository,
        tag_product,
    	tag_management,
    	tag_name,
    	environment,
    	usage_type,
        pricing_term,
    	account_id,
    	account_name,
    	region,
    	sum(cost)/31 as cost
    from dez
    group by 14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
bl as (
    select projection.start_date,
           avg.*
    from avg
    cross join (
        select distinct u.start_date
          from {{ ref('aws_costs_day_usage') }} u
         where u.start_date >= timestamp '2024-12-01'
    ) projection
),
actual as(
    select u.start_date,
        u.resource_id,
    	u.aws_product_name,
    	u.circle_id,
    	u.providedby_id,
        u.tag_repository,
        u.tag_product,
    	u.tag_management,
    	u.tag_name,
    	u.environment,
    	u.usage_type,
        u.pricing_term,
    	u.account_id,
    	u.account_name,
    	u.region,
    	sum(u.reservation_cost + u.unblended_cost) as cost
    from {{ ref('aws_costs_day_usage') }} u
    where u.start_date >= timestamp '2025-01-01'
    group by 15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
),
final as (
    select coalesce(bl.start_date,actual.start_date) as start_date,
            coalesce(bl.resource_id,actual.resource_id) as resource_id,
            coalesce(bl.aws_product_name,actual.aws_product_name) as aws_product_name,
            coalesce(bl.circle_id,actual.circle_id) as circle_id,
            coalesce(bl.providedby_id,actual.providedby_id) as providedby_id,
            coalesce(bl.tag_repository,actual.tag_repository) as tag_repository,
            coalesce(bl.tag_product,actual.tag_product) as tag_product,
            coalesce(bl.tag_management,actual.tag_management) as tag_management,
            coalesce(bl.tag_name,actual.tag_name) as tag_name,
            coalesce(bl.environment,actual.environment) as environment,
            coalesce(bl.usage_type,actual.usage_type) as usage_type,
            coalesce(bl.pricing_term,actual.pricing_term) as pricing_term,
            coalesce(bl.account_id,actual.account_id) as account_id,
            coalesce(bl.account_name,actual.account_name) as account_name,
            coalesce(bl.region,actual.region) as region,
        	sum(actual.cost) as actual_cost,
        	sum(bl.cost) as baseline_cost
    from actual
    full join bl
    on bl.start_date=actual.start_date
    and bl.resource_id=actual.resource_id
    and bl.aws_product_name=actual.aws_product_name
    and bl.circle_id=actual.circle_id
    and bl.providedby_id=actual.providedby_id
    and bl.tag_repository=actual.tag_repository
    and bl.tag_product=actual.tag_product
    and bl.tag_management=actual.tag_management
    and bl.tag_name=actual.tag_name
    and bl.environment=actual.environment
    and bl.usage_type=actual.usage_type
    and bl.pricing_term=actual.pricing_term
    and bl.account_id=actual.account_id
    and bl.account_name=actual.account_name
    and bl.region=actual.region
    group by 15,14,13,12,11,10,9,8,7,6,5,4,3,2,1
)
select * from final
