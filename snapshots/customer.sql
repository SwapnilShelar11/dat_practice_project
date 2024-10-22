{% snapshot customer_snapshot %}

    {{
        config(
            target_schema='retail_consumer', 
            unique_key='customer_id', 
            strategy='check', 
            check_cols=['customer_name', 'email', 'region', 'city', 'state', 'postal_code', 'country', 
            'phone_number', 'preferred_channel', 'loyalty_program_status', 'customer_feedback']  
        )
    }}

    -- Select data from the 'sraw' table (sourced from the retail_raw.customer_raw)
    select
        customer_id,
        customer_name,
        email,
        region,
        city,
        state,
        postal_code,
        country,
        phone_number,
        preferred_channel,
        loyalty_program_status,
        customer_feedback,
        {{ get_utc_timestamp() }} as rec_cre_tms
    from {{ ref("customer_sraw") }}  -- Refers to the 'sraw' table in DBT

{% endsnapshot %}
