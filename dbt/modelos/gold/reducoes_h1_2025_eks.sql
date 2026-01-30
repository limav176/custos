with dez as(
    select u.start_date,
        u.namespace,
    	u.cluster_name,
    	u.environment,
    	u.account_id,
    	u.account_name,
    	u.region,
    	u.circle_id,
    	sum(u.split_cost + u.unused_cost) as cost
    from {{ ref('aws_eks_costs_day_usage') }} u
    where u.start_date >= timestamp '2024-12-01'
      and u.start_date <= timestamp '2024-12-31'
    group by 8,7,6,5,4,3,2,1
),
avg as (
    select
		namespace,
    	cluster_name,
    	environment,
    	account_id,
        account_name,
        region,
    	circle_id,
    	sum(cost)/31 as cost
    from dez
    group by 7,6,5,4,3,2,1
),
bl as (
    select projection.start_date,
           avg.*
    from avg
    cross join (
        select distinct u.start_date
          from {{ ref('aws_eks_costs_day_usage') }} u
         where u.start_date >= timestamp '2024-12-01'
    ) projection
),
actual as(
    select u.start_date,
        u.namespace,
    	u.cluster_name,
    	u.environment,
    	u.account_id,
    	u.account_name,
    	u.region,
    	u.circle_id,
    	sum(u.split_cost + u.unused_cost) as cost
    from {{ ref('aws_eks_costs_day_usage') }} u
    where u.start_date >= timestamp '2025-01-01'
    group by 8,7,6,5,4,3,2,1
),
final as (
    select coalesce(bl.start_date,actual.start_date) as start_date,
            coalesce(bl.namespace,actual.namespace) as namespace,
            coalesce(bl.cluster_name,actual.cluster_name) as cluster_name,
            coalesce(bl.environment,actual.environment) as environment,
            coalesce(bl.account_id,actual.account_id) as account_id,
            coalesce(bl.account_name,actual.account_name) as account_name,
            coalesce(bl.region,actual.region) as region,
            coalesce(bl.circle_id,actual.circle_id) as circle_id,
        	sum(actual.cost) as actual_cost,
        	sum(bl.cost) as baseline_cost
    from actual
    full join bl
    on bl.start_date=actual.start_date
    and bl.namespace=actual.namespace
    and bl.cluster_name=actual.cluster_name
    and bl.environment=actual.environment
    and bl.account_id=actual.account_id
    and bl.account_name=actual.account_name
    and bl.region=actual.region
    and bl.circle_id=actual.circle_id
    group by 8,7,6,5,4,3,2,1
)
select * from final
