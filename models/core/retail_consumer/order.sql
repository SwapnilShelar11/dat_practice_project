{{ config(materialized="table", alias='"ORDER"') }}

select *
from {{ ref("order_sraw") }}
