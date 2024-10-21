{{config(materialized = 'table')}}

select p.* from {{ref("product")}} p
inner join 
{{ref("order")}} o
on p.product_id = o.product_id