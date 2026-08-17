{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        post_hook=apply_classification_tags(
            relation='silver.silver_cash_transaction',
          tags={
            'account_id': 'confidential',
            'transaction_ts': 'internal',
            'amount': 'confidential',
            'description': 'confidential',
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
    select * from {{ source('bronze', 'bronze_cash_transaction') }}

    {% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        ct_ca_id                                          as account_id,
        ct_dts                                             as transaction_ts,
        ct_amt                                             as amount,
        {{ trim_or_null('ct_name') }}                     as description,

        _cdc_flag,
        _cdc_dsn,
        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where ct_ca_id is not null
),

deduped as (
    {{ dedup_latest('cleaned', '_row_hash', '_batch_id desc, _cdc_dsn desc, _loaded_at desc') }}
)

select * from deduped
