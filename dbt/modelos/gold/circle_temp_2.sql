-- with total_circle_costs as (
-- 	select
-- 		sc.origin as origin
--         ,sc.start_date as start_date
-- 		,sum(sc.cost) as cost
-- 	from {{ ref('circle_temp') }} sc
-- 	left join {{ ref('circles') }} c
-- 	on c.circle_id = sc.circle_id
-- 	where c.type = 'stream_aligned'
-- 	group by 1,2
-- ),

-- circles_cost as (
-- 	select
-- 		sc.origin as origin
--         ,sc.start_date as start_date
-- 		,sc.circle_id as circle_id
-- 		,sc.type as type
-- 		,sum(sc.cost) as cost
-- 	from {{ ref('circle_temp') }} sc
-- 	left join {{ ref('circles') }} c
-- 	on c.circle_id = sc.circle_id
-- 	where c.type = 'stream_aligned'
-- 	group by 1,2,3,4
-- ),

-- platform_shared_cost as (
-- 	select
-- 		sc.origin as origin
--         ,sc.start_date as start_date
-- 		,sum(sc.cost) as cost
-- 	from {{ ref('circle_temp') }} sc
-- 	left join {{ ref('circles') }} c
-- 	on c.circle_id = sc.circle_id
-- 	where c.type = 'platform'
-- 	group by 1,2
-- ),

-- circle_platform_rates as (
-- 	select
-- 		cc.origin
--         ,cc.start_date
-- 		,cc.circle_id
-- 		,CAST(cc.cost AS decimal(38,12))/CAST(tcc.cost AS decimal(38,12)) as rate -- % do custo do círculo
-- 		,psc.cost as total_platform_shared_cost
-- 		,psc.cost * CAST(cc.cost AS decimal(38,12))/CAST(tcc.cost AS decimal(38,12)) as platform_shared_cost
-- 	from
-- 		circles_cost cc
-- 	left join
-- 		total_circle_costs tcc
-- 	on
-- 		cc.origin = tcc.origin and cc.start_date = tcc.start_date
-- 	left join
-- 		platform_shared_cost psc
-- 	on
-- 		cc.origin = psc.origin and cc.start_date = psc.start_date
-- ),

-- final as(

--     select
--         origin
--         ,start_date
-- 		,circle_id
-- 		,type
-- 		,cost
-- 	from circles_cost
--     group by 1,2,3,4

--     union all

--     select
--         origin
--         ,start_date
--         ,circle_id
--         ,'platform' type
--         ,sum(platform_shared_cost) as cost
--     from circle_platform_rates
--     group by 1,2,3,4
-- )


-- Custos totais de plataforma a ser rateado
with platform_total_cost as (
	select
		sc.origin as origin
        ,sc.start_date as start_date
		,sum(sc.cost) as cost
	from {{ ref('circle_temp') }} sc
	left join {{ ref('circles') }} c
	on c.circle_id = sc.circle_id
	where c.type = 'platform'
	group by 1,2
),

-- Rate por circulo
rates as (
	select distinct
		sc.origin as origin
        ,sc.start_date as start_date
        ,sc.circle_id
		,SUM(cost) OVER(PARTITION BY sc.origin, sc.start_date, sc.circle_id) as cost
		,SUM(cost) OVER(PARTITION BY sc.origin, sc.start_date) as total_cost
		,CAST(SUM(cost) OVER(PARTITION BY sc.origin, sc.start_date, sc.circle_id) AS DECIMAL(38,20))/CAST(SUM(cost) OVER(PARTITION BY sc.origin, sc.start_date) AS DECIMAL(38,20)) as rate
	from {{ ref('circle_temp') }} sc
	left join {{ ref('circles') }} c
	on c.circle_id = sc.circle_id
	where c.type = 'stream_aligned'
),

-- Valores por circulo
shares as(
SELECT
    t.origin as origin
    ,t.start_date as start_date
    ,r.circle_id as circle_id
    ,r.rate * t.cost as cost
FROM
    platform_total_cost t
LEFT JOIN
    rates r
ON
    t.start_date = r.start_date AND t.origin = r.origin
),

final as(
    -- usage, shared, support, untagged
	select
		sc.origin as origin
        ,sc.start_date as start_date
		,sc.circle_id as circle_id
		,sc.type as type
		,sum(sc.cost) as cost
	from {{ ref('circle_temp') }} sc
	left join {{ ref('circles') }} c
	on c.circle_id = sc.circle_id
	where c.type = 'stream_aligned'
	group by 1,2,3,4

    union all

    -- platform
    select
        origin
        ,start_date
        ,circle_id
        ,'platform' type
        ,sum(cost) as cost
    from shares
    group by 1,2,3,4
)

select * from final
