-- macros/check_null_values.sql
{% macro check_null_values(model_name, columns) %}
    -- Generate the SQL logic to check for NULL values in the specified columns
    {% set conditions = [] %}
    {% for column in columns %}
        {% set condition = column ~ ' IS NULL' %}
        {% do conditions.append(condition) %}
    {% endfor %}

    -- Create the final SQL query
    {% set query %}
        SELECT 
            {{ columns | join(', ') }}
        FROM {{ ref(model_name) }}
        WHERE 
            {{ conditions | join(' OR ') }}
    {% endset %}

    -- Return the query
    {{ return(query) }}
{% endmacro %}
