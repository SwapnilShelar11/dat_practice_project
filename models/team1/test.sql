{{config(materialized = 'table')}}
select * from {{ref('test1')}} where cc_manager = 'Wayne Ray'