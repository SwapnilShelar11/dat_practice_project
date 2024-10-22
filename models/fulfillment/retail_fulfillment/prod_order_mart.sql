{{config(materialized = 'table')}}

with product_metrics as (
    select
        product_id,
        count(order_id) as total_orders,  -- Total number of orders for this product
        sum(quantity) as total_units_sold,  -- Total units sold for this product
        sum(order_amount) as total_sales_value,  -- Total sales value for this product
        avg(order_amount) as avg_order_value,  -- Average order value per product
        min(order_date) as first_sale_date,  -- Date of first sale
        max(order_date) as last_sale_date,  -- Date of last sale
        datediff('day', min(order_date), max(order_date)) as sale_window_days  -- Time between first and last sale
    from {{ref("order")}}
    group by product_id
),

-- Step 4: Join product data with product performance metrics
combined_data as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        p.stock_quantity,
        pm.total_orders,  -- Total number of orders for the product
        pm.total_units_sold,  -- Total units sold
        pm.total_sales_value,  -- Total sales value
        pm.avg_order_value,  -- Average order value for the product
        pm.first_sale_date,  -- First sale date for the product
        pm.last_sale_date,  -- Last sale date for the product
        pm.sale_window_days,  -- Number of days between first and last sale
        case 
            when pm.total_units_sold > p.stock_quantity then 'Out of Stock'
            when pm.sale_window_days > 365 then 'Stale Product'
            when pm.sale_window_days between 30 and 365 then 'Active'
            else 'New Product'
        end as product_lifecycle_stage,  -- Derived lifecycle stage based on stock and sales history
        {{ get_utc_timestamp() }} as rec_cre_tms  -- Mart creation timestamp
    from {{ref("product")}} p
    left join product_metrics pm on p.product_id = pm.product_id
)

-- Step 5: Filter for only active or sold products (example rule: total_units_sold > 0)
select *
from combined_data
where total_units_sold > 0