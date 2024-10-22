{{ config(materialized="table") }}

with
    raw_data as (
        select
            customer_id::int as customer_id,
            customer_name::string as customer_name,
            email::string as email,
            region::string as region,
            city::string as city,
            state::string as state,
            postal_code::string as postal_code,
            country::string as country,
            phone_number::string as phone_number,
            preferred_channel::string as preferred_channel,
            loyalty_program_status::string as loyalty_program_status,
            customer_feedback::string as customer_feedback
        from {{ source("retail_raw", "customer_raw") }}
    )
select *,
{{ get_utc_timestamp() }} as rec_cre_tms 
from raw_data
where customer_id is not null
