{{ config(materialized="table") }}

with
    raw_data as (
        select
            order_id::int as order_id,
            customer_id::int as customer_id,
            product_id::int as product_id,
            order_date::date as order_date,
            quantity::int as quantity,
            order_amount::decimal(10, 2) as order_amount
        from {{ source("retail_raw", "order_raw") }}
    )
select *, {{ get_utc_timestamp() }} as rec_cre_tms
from raw_data
where order_id is not null
