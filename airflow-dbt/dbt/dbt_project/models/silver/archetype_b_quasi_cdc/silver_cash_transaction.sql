{#-
    Quasi-CDC append-only event log (bronze design doc, open question #3).
    CDC_FLAG on this source doesn't reflect genuine state updates, so we
    do NOT treat it as "latest CDC_DSN wins" for state reconstruction --
    every row is an independent event, not a revision of a prior row.

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

    Tie-break order when a true duplicate is found: _batch_id desc,
    _cdc_dsn desc, _loaded_at desc -- keep the most recently ingested
    copy. _batch_id leads (not _cdc_dsn) because it's guaranteed to
    reflect true calendar order via bronze_batch_control.asofdate, while
    _cdc_dsn's global monotonicity across the whole CDC stream (vs. reset
    per batch/file) is unconfirmed as of this writing (see silver.md,
    open question #4). _loaded_at is the final tie-breaker for true ties.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_cash_transaction') }}
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