WITH source as (
    select start_date
        ,circle_id
        ,journey
        ,template
        ,case when lower(type)='email_with_attachment' then 'EMAIL' else type end as type
        ,case when lower(type)='email_with_attachment' then true else false end as has_attachment
        ,qty
    from  {{ source('notificacao_transacional_gold', 'circle_notification_usage') }}
    where start_date >= timestamp '2025-01-01' and circle_id not like '%/%'
),

usage as(
    select distinct
        start_date
        ,circle_id
        ,journey
        ,template
        ,type
        ,has_attachment
        ,SUM(qty) OVER(PARTITION BY start_date,circle_id,journey,template,type) as send_qty
        ,SUM(qty) OVER(PARTITION BY start_date,type) as send_total_qty
        ,SUM(qty) OVER(PARTITION BY start_date,circle_id,journey,template,type) / SUM(qty) OVER(PARTITION BY start_date,type) as send_daily_usage
        ,SUM(qty) OVER(PARTITION BY start_date,circle_id,journey,template,type,has_attachment) as attach_qty
        ,SUM(qty) OVER(PARTITION BY start_date,type,has_attachment) as attach_total_qty
        ,SUM(qty) OVER(PARTITION BY start_date,circle_id,journey,template,type,has_attachment) / SUM(qty) OVER(PARTITION BY start_date,type,has_attachment) as attach_daily_usage
    from  source
),

ses_costs as (
    select start_date
        ,usage_type
        ,unblended_cost
        ,reservation_cost
    from {{ ref('aws_costs_day_usage') }}
    where aws_product_name = 'Amazon Simple Email Service'
    and start_date >= timestamp '2025-01-01'

    union all

    select start_date
        ,usage_type
        ,unblended_cost
        ,reservation_cost
    from {{ ref('aws_costs_day_untagged') }}
    where aws_product_name = 'Amazon Simple Email Service'
    and start_date >= timestamp '2025-01-01'
),

costs as (
    select 'EMAIL' as type
        ,date_trunc('day',start_date) as start_date
        ,sum(case when lower(usage_type) like '%attachment%'
            then unblended_cost + reservation_cost
            else 0 end) as attach_daily_cost
        ,sum(case when lower(usage_type) not like '%attachment%'
            then unblended_cost + reservation_cost
            else 0 end) as send_daily_cost
    from ses_costs
    group by 1,2
)

select u.start_date
        ,u.circle_id
        ,u.journey
        ,u.template
        ,case when lower(u.type)='email' and u.has_attachment=true
            then 'EMAIL_WITH_ATTACHMENT'
        else u.type end as type
        ,u.send_qty
        ,u.attach_qty
        ,case when lower(u.type)='email' and u.has_attachment=true then
            (u.send_daily_usage * co.send_daily_cost) + (u.attach_daily_usage * co.attach_daily_cost)
        when lower(u.type)='email' and u.has_attachment=false then
            (u.send_daily_usage * co.send_daily_cost)
        when lower(u.type)='sms' then
            u.send_qty * (0.045 / 6.0085) --custo unitario twilio / dolar
        end as cost
from usage u
left join costs co
on lower(u.type) = lower(co.type) and u.start_date = co.start_date
left join {{ ref('circles') }} c
on c.circle_id = u.circle_id
where c.circle_name is not null
