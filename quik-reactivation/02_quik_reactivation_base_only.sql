with live_customer_stats as (
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
    b.merchant_name,
    c.is_uae
    from
    quik_users a
    left join last_merchant b
        on a.customer_id = b.customer_id
    left join prod_helper.customer c 
        on a.customer_id = c.customer_id
)
select * from quik_churned_base
where
is_uae = True  -- residents only
and quik_delivered_orders_LT>=5 -- graduated only
