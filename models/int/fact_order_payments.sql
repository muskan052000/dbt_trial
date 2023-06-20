with data as (
     select 
     id as order_id,
    user_id as customer_id,
     status,
     order_date
from {{ref("fct_orders")}}
)
select * from data

-- customer as (
--     select 

-- from {{ref("stg_payments")}}    
-- )