{{
    config(
        materialized="incremental",
        unique_key=[
            "order_id",
            "customer_id",
            "product_id",
        ],
    )
}}

select order_id, customer_id, product_id, order_date, quantity, order_amount
from {{ ref("order_sraw") }}
