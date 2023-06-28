with fact_cohort as (
select
    -- rollup
    a.customer_id,
    -- Aggregate 
    min(case when a.order_status = 'completed' and b.status = 'success' then a.order_date end) as first_order_date,
    max(a.order_date) as last_order_date,
    count (distinct a.order_id) as total_orders,
    sum(a.amount) as total_revenue,
    round((max(a.order_date) - min(a.order_date)) / 
        count (distinct a.order_id)) as average_days_between_orders,
    current_timestamp as created_at
from {{ref("fct_orders")}} as a
join {{ref("stg_payments")}} as b
on a.order_id = b.order_id
group by 1
order by 1 asc)

select * from fact_cohort