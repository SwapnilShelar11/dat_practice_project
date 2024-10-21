{{config(materialized = 'table')}}

select c.* from {{ref("customer")}} c
inner join 
{{ref("order")}} o
on c.customer_id = o.customer_id