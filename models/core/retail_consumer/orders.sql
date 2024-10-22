{{
    config(
        materialized="incremental"
    )
}}

{% set is_table_present = not (dbt_utils.get_relations_by_prefix(schema=target.schema, prefix=this.name) | length == 0) %}

{% if is_table_present %}
    with max_rec_cre_tms as (
        select max(rec_cre_tms) as max_tms
        from {{ this }}
    )
{% endif %}

select 
    order_id, 
    customer_id, 
    product_id, 
    order_date, 
    quantity, 
    order_amount,
    {{ get_utc_timestamp() }} as rec_cre_tms
from {{ ref("order_sraw") }}
{% if is_incremental() and is_table_present %}
where rec_cre_tms > coalesce((select max_tms from max_rec_cre_tms), '1900-01-01')
{% endif %}
