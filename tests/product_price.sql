-- tests/product_price.sql

-- Test to ensure stock_quantity is non-negative
with negative_stock as (
    select *
    from {{ ref('product') }}
    where stock_quantity < 0
)

select
    count(*) as error_count
from negative_stock