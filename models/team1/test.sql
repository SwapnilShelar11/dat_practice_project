{{config(materialized = 'table')}}
select * from dev.test.call_center where cc_manager = 'Wayne Ray'