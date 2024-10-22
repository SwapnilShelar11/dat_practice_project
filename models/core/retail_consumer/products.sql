{{ config(materialized="incremental", unique_key="product_id") }}

select
    product_id,
    product_name,
    category,
    case
        when price < 0
        then null  -- Handle negative prices by setting them to null
        else price
    end as price,
    stock_quantity
from {{ ref("product_sraw") }}
where product_id is not null 
