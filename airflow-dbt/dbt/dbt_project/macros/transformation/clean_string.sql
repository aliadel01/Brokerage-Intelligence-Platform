{% macro trim_or_null(column_name, uppercase=false) %}
{#- Simpler, safe version: trim, blank -> null, optional uppercase. -#}
    {% set expr = "trim(" ~ column_name ~ ")" %}
    {% if uppercase %}
        nullif(upper({{ expr }}), '')
    {% else %}
        nullif({{ expr }}, '')
    {% endif %}
{% endmacro %}