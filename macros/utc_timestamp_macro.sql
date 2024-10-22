{% macro get_utc_timestamp() %}
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
{% endmacro %}
