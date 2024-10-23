{{
    config(
        materialized="incremental"
    )
}}


with max_record_added_timestamp as (
        select max(record_added_timestamp) as max_tms
        from {{ this }}
    )

select 
    order_id, 
    customer_id, 
    product_id, 
    order_date, 
    quantity, 
    order_amount,
    record_added_timestamp, 
    {{ get_utc_timestamp() }} as rec_cre_tms
from {{ ref("order_sraw") }}
{% if is_incremental() %}
where record_added_timestamp > coalesce((select max_tms from max_record_added_timestamp), '1900-01-01')
{% endif %}
