-- Daily revenue by product category and acquisition channel.
-- This is the mart marketing and finance both look at, so it gets tested hard.

with sessions as (

    select * from {{ source('silver', 'sessions') }}

),

daily as (

    select
        cast(session_start as date)        as activity_date,
        category,
        count(distinct session_id)         as sessions,
        count(distinct user_id)            as users,
        sum(revenue)                       as revenue,
        sum(case when revenue > 0 then 1 else 0 end) as converting_sessions

    from sessions
    group by 1, 2

)

select
    *,
    round(converting_sessions * 1.0 / nullif(sessions, 0), 4) as conversion_rate
from daily
