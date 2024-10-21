{{config(materialized = 'table')}}

select * from {{source('retail_raw','customer_raw')}}