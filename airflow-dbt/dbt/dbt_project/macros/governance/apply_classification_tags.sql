{% macro apply_classification_tags(relation , tags={}) %}
  {% set stmts = [] %}
  {% for col, level in tags.items() %}
    {% do stmts.append("ALTER TABLE " ~ relation  ~ " MODIFY COLUMN " ~ col ~ " SET TAG brokerage_dwh.governance.data_classification = '" ~ level ~ "'") %}
  {% endfor %}
  {{ return(stmts) }}
{% endmacro %}