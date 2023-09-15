{{config(materialized = 'table')}}
select * from {{ref('test1')}} where cc_state = 'LA'