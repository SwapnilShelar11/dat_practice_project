{{ config(materialized="table") }}

with
    raw_data as (
        select
            product_id::int as product_id,
            product_name::string as product_name,
            category::string as category,
            price::decimal(10, 2) as price,
            stock_quantity::int as stock_quantity
        from {{ source("retail_raw", "product_raw") }}
    )
select *,
{{ get_utc_timestamp() }} as rec_cre_tms
from raw_data
where product_id is not null

