{{config(materialized = 'table')}}

select * from {{ref("product_sraw")}}