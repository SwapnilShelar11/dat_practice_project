{{
    config(
        materialized="incremental",
        unique_key="product_id",
    )
}}

with max_record_added_timestamp as (
        select max(record_added_timestamp) as max_tms from {{ this }}
    )

select
    product_id,
    product_name,
    category,
    case
        when price < 0
        then null  -- Handle negative prices by setting them to null
        else price
    end as price,
    stock_quantity,
    record_added_timestamp,
    {{ get_utc_timestamp() }} as rec_cre_tms
from {{ ref("product_sraw") }}
where
    product_id is not null
    {% if is_incremental() %}
        and record_added_timestamp > coalesce(
            (select max_tms from max_record_added_timestamp),
            to_timestamp('1900-01-01', 'YYYY-MM-DD')
        )
    {% endif %}
