{{
    config(
        materialized="incremental",
        unique_key="product_id",
    )
}}

{% set is_table_present = not (dbt_utils.get_relations_by_prefix(schema=target.schema, prefix=this.name) | length == 0) %}

with max_rec_cre_tms as (
    {% if is_incremental() and is_table_present %}
        select max(rec_cre_tms) as max_tms
        from {{ this }}
    {% else %}
        select null as max_tms  -- Handle the case where the table is not present
    {% endif %}
),

product_cte as (
    select
        product_id,
        product_name,
        category,
        case
            when price < 0 then null  -- Handle negative prices by setting them to null
            else price
        end as price,
        stock_quantity,
        row_number() over (partition by product_id order by rec_cre_tms desc) as rn
    from {{ ref("product_sraw") }}
    where product_id is not null
    {% if is_incremental() and is_table_present %}
        and rec_cre_tms > coalesce((select max_tms from max_rec_cre_tms), '1900-01-01')
    {% endif %}
)

select 
    product_id,
    product_name,
    category,
    price,
    stock_quantity,
    {{ get_utc_timestamp() }} as rec_cre_tms
from product_cte
where rn = 1
