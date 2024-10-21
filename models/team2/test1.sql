{{config(materialized = 'table')}}
select * from {{ref('test')}} where cc_country = 'United States'