-- macros/apply_pii_masking.sql
{% macro apply_pii_masking(relation, string_cols=[], date_cols=[], numeric_cols=[]) %}
  {% set stmts = [] %}
  {% for c in string_cols %}
    {% do stmts.append("ALTER TABLE " ~ relation  ~ " MODIFY COLUMN " ~ c ~ " SET MASKING POLICY brokerage_dwh.governance.mask_pii_string") %}
  {% endfor %}
  {% for c in date_cols %}
    {% do stmts.append("ALTER TABLE " ~ relation  ~ " MODIFY COLUMN " ~ c ~ " SET MASKING POLICY brokerage_dwh.governance.mask_pii_date") %}
  {% endfor %}
  {% for c in numeric_cols %}
    {% do stmts.append("ALTER TABLE " ~ relation  ~ " MODIFY COLUMN " ~ c ~ " SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric") %}
  {% endfor %}
  {{ return(stmts) }}
{% endmacro %}