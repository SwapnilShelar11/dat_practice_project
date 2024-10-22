{{config(materialized = 'table')}}

with order_metrics as (
    select
        customer_id,
        count(order_id) as total_orders,  -- Total number of orders
        sum(order_amount) as total_order_value,  -- Total value of orders
        avg(order_amount) as avg_order_value,  -- Average order value
        min(order_date) as first_order_date,  -- Date of first order
        max(order_date) as last_order_date,  -- Date of last order
        datediff('day', min(order_date), max(order_date)) as order_window_days  -- Time between first and last order
    from {{ref("orders")}}
    group by customer_id
),

combined_data as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.region,
        c.city,
        c.state,
        c.postal_code,
        c.country,
        c.phone_number,
        c.preferred_channel,
        c.loyalty_program_status,
        coalesce(om.total_orders, 0) as total_orders,  -- Total orders per customer
        coalesce(om.total_order_value, 0) as total_order_value,  -- Total order value per customer
        coalesce(om.avg_order_value, 0) as avg_order_value,  -- Average order value per customer
        om.first_order_date,  -- First order date
        om.last_order_date,  -- Last order date
        om.order_window_days,  -- Number of days between first and last order
        case 
            when om.order_window_days < 30 then 'New'
            when om.order_window_days between 30 and 365 then 'Returning'
            else 'Loyal' 
        end as customer_lifecycle_stage,  -- Derived lifecycle stage based on order history
        {{ get_utc_timestamp() }} as rec_cre_tms  -- Mart creation timestamp
    from {{ref("customer_snapshot")}} c
    left join order_metrics om on c.customer_id = om.customer_id
    where dbt_valid_to is null
)

-- Step 5: Filter for only active customers (example rule: total_orders > 0)
select *
from combined_data
where total_orders > 0