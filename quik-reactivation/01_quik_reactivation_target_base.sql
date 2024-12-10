-- 1 get quik orders for the day
-- 2 last quik order should be more than 30 days ago

-- all customers with orders today
-- with orders_today as (
--     select 
--         distinct careem_user_id as customer_id, coupon
--     from 
--         careem_now.orders__orders as o 
--         join dev_bi.quik_merchants as m 
--             on o.merchant_id = m.merchant_id 

--     where 
--         lower(status)='delivered' 
--         and o.day = date '{anchor_date}'
--         and o.city_id IN (1, 2)
--         and domain = 'shops'
-- )
with cplus_users as (
select
  customer_id, member_type, 1 as cplus_user
  from
  (
    select
      distinct
      cast(substr(userid, 6) as bigint) customer_id,
      case 
          when lower(subscriptiontype) in ('regular') then 'paid'   -- Paid Renewals
          when planid in ('23','39','43','55','64') then 'paid'                --  B2B Plans 
          when lower(invoiceid) like '%free_renewal%' then 'paid'   -- Free renewals rewarded
          when lower(subscriptiontype) like '%trial%commitment%' then  'trial_without_commitment'
          when lower(subscriptiontype) like '%trial' then  'trial'
      end as member_type,
      row_number() over (partition by cast(substr(userid, 6) as bigint) order by day desc) as rn
    from
      careem_s3_incremental.ddb_prod_subscription_events
    where
      lower(eventtype) = 'subscription_started'
      and cast(expiresat as date) >= date '{anchor_date}'
      and cast(startedat as date) < date '{anchor_date}'
      and serviceareaid in ('1', '21')
  )
  where rn=1
),

nationality as (
select
    CAST(customer_id AS BIGINT) AS customer_id,
    account_predicted_nationality as nationality,
    -- CAST(account_predicted_nationality_score as double) as confidence,
    account_region_based_on_nationality as region,
    account_subregion_based_on_nationality as sub_region,
    platform_is_loyal_customer
    from shared.customer_profile_dim
),

live_customer_stats as (
    select 
        careem_user_id as customer_id,
        min(day) as first_order_date,
        max(day) as last_order_date,
        count(distinct id) as delivered_orders,
        -- count(distinct case when day >= current_date - interval '30' day then id else null end) as delivered_orders_0_30,
        -- count(distinct case when day = current_date then id else null end) as daily_delivered_orders,
        date_diff('day', max(day), date '{anchor_date}') as quik_recency
    from 
        careem_now.orders__orders as o 
        join dev_bi.quik_merchants as m 
            on o.merchant_id = m.merchant_id 

    where 
        lower(status)='delivered' 
        and o.day >= date '2021-11-01'
        and o.day < date '{anchor_date}'
        and o.city_id IN (1, 2)
        and domain = 'shops'
    group by 1
),

quik_users as (

    select
        current_date as day,
        customer_id,
        first_order_date,
        last_order_date,
        delivered_orders,
        -- delivered_orders_currenrders,
        quik_recency
    from
        live_customer_stats
    where
        quik_recency > 30
),

last_merchant as (

    select
        customer_id,
        merchant_name
    from
    
        (
    
            select 
                careem_user_id AS customer_id,
                m.merchant_name,
                ROW_NUMBER() OVER (PARTITION BY careem_user_id ORDER BY o.created_at DESC) AS rn
            from 
                careem_now.orders__orders as o 
                join dev_bi.quik_merchants as m 
                    on o.merchant_id = m.merchant_id 
            where 
                LOWER(status) = 'delivered' 
                and o.day >= date '2021-11-01'
                and o.day < date '{anchor_date}'
                and o.city_id in (1, 2)
                and domain = 'shops'
        )
    where
        rn = 1
),

quik_churned_base as (
  select
    a.day,
    a.customer_id,
    quik_recency,
    first_order_date,
    last_order_date,
    case 
        when delivered_orders = 0 then '0'
        when delivered_orders = 1 then '1'
        when delivered_orders BETWEEN 2 and 4 then '2-4'
        when delivered_orders BETWEEN 5 and 10 then '5-10'
        when delivered_orders > 10 then '10+'
    end as quik_tranx_cohort,
    case 
        when quik_recency <= 45 then '30-45' 
        when quik_recency <= 60 then '45-60' 
        when quik_recency <= 90 then '60-90'
        when quik_recency <= 120 then '90-120' 
    else '120+' end as quik_last_order_segment,
    delivered_orders as quik_delivered_orders_LT,
    b.merchant_name
    from
    quik_users a
    left join last_merchant b
        on a.customer_id = b.customer_id
),

sa_users as (
  select
    distinct
    a.customer_id,
    monthly_status,
    careem_all_time_tx as sa_careem_all_time_tx,
    case when careem_all_time_tx = 0 then '0'
         when careem_all_time_tx BETWEEN 1 and 10 then '1-10'
         when careem_all_time_tx BETWEEN 11 and 20 then '11-20'
         when careem_all_time_tx BETWEEN 21 and 50 then '21-50'
         when careem_all_time_tx > 50 then '50+'
    end as sa_tranx_cohort,
    case 
        when date_diff('day', careem_last_tx_date, day) IS NULL then 'n/a'
        when date_diff('day', careem_last_tx_date, day) BETWEEN 0 and 7 then '0-7' 
        when date_diff('day', careem_last_tx_date, day) BETWEEN 8 and 14 then '8-14' 
        when date_diff('day', careem_last_tx_date, day) BETWEEN 15 and 21 then '15-21'
        when date_diff('day', careem_last_tx_date, day) BETWEEN 22 and 30 then '22-30'
        when date_diff('day', careem_last_tx_date, day) BETWEEN 31 and 45 then '31-45'
        when date_diff('day', careem_last_tx_date, day) BETWEEN 46 and 60 then '46-60'
        when date_diff('day', careem_last_tx_date, day) BETWEEN 61 and 90 then '61-90'
        when date_diff('day', careem_last_tx_date, day) BETWEEN 91 and 120 then '91-120'
        else '120+' end as sa_last_order_before
    
    from 
    sa_prod_agg.customer_status_details a
  where 
    -- a.day = date '{anchor_date}' 
    -- a.day = (select max(day) from sa_prod_agg.customer_status_details where day >= current_date - interval '2' day)
    a.day = (select max(day) from sa_prod_agg.customer_status_details where (day >= date '{anchor_date}' - interval '3' day) and (day < date '{anchor_date}'))
    and a.granularity = 'city'
    and a.city = 'Dubai'
),

food_users as (
    select 
        distinct
        customer_id,
        monthly_status,
        last_order_date as food_last_order_date,
        delivered_orders as food_delivered_orders_LT
    from 
        now_prod_agg.customer_Status_details
    where 
        -- day = date '{anchor_date}' 
        day = (select max(day) from now_prod_agg.customer_Status_details where (day >= date '{anchor_date}' - interval '3' day) and (day < date '{anchor_date}'))
        and granularity = 'city'
        and city in ('Dubai')
        and domain = 'food'
        and level = 'service'
),

food_gp as (
  select
    distinct customer_id,
    count(distinct order_id) as n_food_orders,
    count(distinct case when DATE_DIFF('day', order_received_timestamp, date '{anchor_date}') <= 30 then order_id end) as n_food_orders_30,
    
    max(order_received_timestamp) as last_food_order_date,
    sum(gtv) as food_gmv,
    sum(gtv - cogs - partner_share) as food_gp,
    sum(gtv - cogs - partner_share) / nullif(count(distinct order_id), 0) as food_gppo
  from
    dev_bi.food_order_lineitems_level f
  where
    1 = 1 
    and order_received_timestamp < date '{anchor_date}'
    --   and date = current_date - interval '1' day
    -- and customer_id = 52919398
  group by
    1
),

quik_gp as (
  select
    distinct
    q.customer_id,
    count(distinct order_id) as n_quik_orders,
    sum(gmv) as quik_gmv,
    sum(net_revenue + delivery_costs + wastage) as quik_gp,
    sum(net_revenue + delivery_costs + wastage) / nullif(count(distinct order_id),0) as quik_gppo
  from
    dev_bi.quik_NR_order_level3 q
    where
    1 = 1
    and day < date '{anchor_date}'
group by 1
),

rides_gp as (
    with trip_unions as (
    select
        distinct
        t.day,
        t.customer_id,
        t.trip_id,
        t.revenue,
        t.cogs
    from
        prod_dwh.trip t
    where
        1 = 1
        and day BETWEEN DATE '2012-01-01' AND DATE '2015-12-31' -- dates parameter set so it doesn't exceed 2000 partition issues
        and lower(business_type) in ('ride hailing', 'jv')
        and trip_outcome = 3
        and lower(country) in ('united arab emirates')
    
    UNION ALL

    select
        distinct
        t.day,
        t.customer_id,
        t.trip_id,
        t.revenue,
        t.cogs
    from
        prod_dwh.trip t
    where
        1 = 1
        and day BETWEEN DATE '2016-01-01' AND DATE '2019-12-31' -- dates parameter set so it doesn't exceed 2000 partition issues
        and lower(business_type) in ('ride hailing', 'jv')
        and trip_outcome = 3
        and lower(country) in ('united arab emirates')
    
    UNION ALL

    select
        distinct
        t.day,
        t.customer_id,
        t.trip_id,
        t.revenue,
        t.cogs
    from
        prod_dwh.trip t
    where
        1 = 1
        and day BETWEEN DATE '2020-01-01' and DATE '2023-12-31' -- dates parameter set so it doesn't exceed 2000 partition issues
        and lower(business_type) in ('ride hailing', 'jv')
        and trip_outcome = 3
        and lower(country) in ('united arab emirates')
        
    UNION ALL

    select
        distinct
        t.day,
        t.customer_id,
        t.trip_id,
        t.revenue,
        t.cogs
    from
        prod_dwh.trip t
    where
        1 = 1
        and day >= DATE '2024-01-01' -- dates parameter set so it doesn't exceed 2000 partition issues. End date not taken here.
        and day < DATE '{anchor_date}'
        and lower(business_type) in ('ride hailing', 'jv')
        and trip_outcome = 3
        and lower(country) in ('united arab emirates')
    )

    select
        distinct
        customer_id,
        max(day) as last_trip_date,
        count(distinct trip_id) as n_trips,
        count(distinct case when DATE_DIFF('day', day, date '{anchor_date}') <= 30 then trip_id end) as n_trips_30,
        sum(revenue) as rides_gmv,
        sum(revenue - cogs) as rides_gp,
        sum(revenue - cogs) / nullif(count(distinct trip_id) ,0) as rides_gppo
    from
        trip_unions
    group by 1
),
sa_gp as (
select
    distinct sa.*,
    -- sa.quik_recency,
    -- sa.first_order_date,
    -- sa.last_order_date,
    -- sa.quik_delivered_orders_LT,
    -- sa.merchant_name,
    coalesce(n_food_orders_30, 0) as n_food_orders_30,
    coalesce(food_gmv, 0) as food_gmv,
    coalesce(n_trips_30, 0) as n_trips_30,
    coalesce(rides_gmv, 0) as rides_gmv,
    coalesce(DATE_DIFF('day', last_food_order_date, date '{anchor_date}'), 365) AS food_recency,
    coalesce(DATE_DIFF('day', last_trip_date, date '{anchor_date}'), 365) AS rides_recency,
    coalesce(rgp.n_trips, 0) as n_trips,
    coalesce(rgp.rides_gp, 0) as rides_gp,
    coalesce(rgp.rides_gppo, 0) as rides_gppo,
    coalesce(fgp.n_food_orders, 0) as n_food_orders,
    coalesce(fgp.food_gp, 0) as food_gp,
    coalesce(fgp.food_gppo, 0) as food_gppo,
    coalesce(qgp.n_quik_orders, 0) as n_quik_orders,
    coalesce(qgp.quik_gmv, 0) as quik_gmv,
    coalesce(qgp.quik_gp, 0) as quik_gp,
    coalesce(qgp.quik_gppo, 0) as quik_gppo,
    coalesce(rgp.rides_gmv,0) + coalesce(fgp.food_gmv,0) + coalesce(qgp.quik_gmv, 0) as sa_gmv,
    -- ((coalesce(rgp.rides_gp,0) + coalesce(fgp.food_gp,0) + coalesce(qgp.quik_gp, 0)) / (coalesce(rgp.n_trips,0) + coalesce(fgp.n_food_orders, 0) + coalesce(qgp.n_quik_orders, 0))) as sa_gppo,
    c.is_uae,
    coalesce(cplus_user, 0) as cplus_user,
    cplus_users.member_type,
    nationality,
    region,
    sub_region,
    platform_is_loyal_customer
from
    quik_churned_base sa
    left join rides_gp rgp
        on sa.customer_id = rgp.customer_id
    left join food_gp fgp
        on sa.customer_id = fgp.customer_id
    left join quik_gp qgp
        on sa.customer_id = qgp.customer_id
    left join prod_helper.customer c 
        on sa.customer_id = c.customer_id
    left join 
    cplus_users
    on sa.customer_id = cplus_users.customer_id
    left join 
    nationality
    on sa.customer_id = nationality.customer_id
where
    1=1
    -- and is_uae = True
)
select * from sa_gp
where 
is_uae = True
and 
n_quik_orders >=5