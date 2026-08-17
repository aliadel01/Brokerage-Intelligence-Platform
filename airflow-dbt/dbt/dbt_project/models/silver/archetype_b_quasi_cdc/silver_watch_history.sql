{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        post_hook=apply_classification_tags(
            relation='silver.silver_watch_history',
          tags={
            'customer_id': 'confidential',
            'security_symbol': 'public',
            'event_ts': 'internal',
            'watch_action': 'internal',
            '_cdc_flag': 'internal',
            '_cdc_dsn': 'internal',
            '_batch_id': 'internal',
            '_row_hash': 'internal',
            '_loaded_at': 'internal'
          }
        )
    )
}}

{#-
    Quasi-CDC append-only event log (bronze design doc).

    Dedup is by _row_hash only, catching TRUE duplicate ingestion (same
    business content landed more than once) -- not by any business key,
    since a legitimate source is expected to send at most one row per
    event. This dedup exists as a defense-in-depth / idempotency
    safeguard at the silver layer itself: the ingestion layer already
    guards against reloading the same batch twice (see ingestion layer
    doc, batch idempotency check via bronze_batch_control), but that
    guard operates at the batch level, not the row level, and doesn't
    protect against a source vendor sending the same event twice by
    mistake within a single extract. Each layer verifies its own
    invariants rather than blindly trusting an upstream layer's
    guarantee.

    INCREMENTAL: append-only, no unique_key. Each source row is an
    independent event -- never a revision of a prior row -- so there is
    nothing to upsert against. On incremental runs we only pull source
    rows newer than the max _loaded_at already in this table, then dedup
    that new slice the same way as a full build.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_watch_history') }}

    {% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        w_c_id                                           as customer_id,
        {{ trim_or_null('w_s_symb', uppercase=true) }}  as security_symbol,
        w_dts                                             as event_ts,
        {{ trim_or_null('w_action', uppercase=true) }}  as watch_action,

        _cdc_flag,
        _cdc_dsn,
        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where w_c_id is not null
),

deduped as (
    {{ dedup_latest('cleaned', '_row_hash', '_batch_id desc, _cdc_dsn desc, _loaded_at desc') }}
)

select * from deduped
