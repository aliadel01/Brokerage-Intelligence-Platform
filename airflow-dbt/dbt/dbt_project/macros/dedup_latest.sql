{% macro dedup_latest(relation, partition_by, order_by) %}
{#-
    Collapses a CTE/relation down to one row per partition_by key, keeping
    the row that sorts first per order_by (pass DESC for "latest wins").
    Uses Snowflake QUALIFY so it can be appended directly after a SELECT.

    Usage:
        select *
        from cleaned
        {{ dbt_utils.dedup_latest? }}  -- not a real dbt_utils macro, ours:
        qualify ... is generated inline below, so call as a full replacement:

        {{ dedup_latest('cleaned', 'ca_id', '_cdc_dsn desc, _batch_id desc, _loaded_at desc') }}
-#}
    select *
    from {{ relation }}
    qualify row_number() over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    ) = 1
{% endmacro %}
