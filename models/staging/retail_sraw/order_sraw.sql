{{config(materialized = 'table')}}

select * from {{source('retail_raw','order_raw')}}