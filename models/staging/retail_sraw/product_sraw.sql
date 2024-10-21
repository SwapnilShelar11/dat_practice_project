{{config(materialized = 'table')}}

select * from {{source('retail_raw','product_raw')}}