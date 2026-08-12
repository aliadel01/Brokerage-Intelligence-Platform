-- =============================================================================
-- Point-in-time reconstruction: silver_account
-- Grain: one row per account_id per day (valid_from_date / valid_to_date),
-- surrogate key = account_version_sk (ADR-001, 04_silver.md).
--
-- Returns the version of every account that was active on :as_of_date.
--
-- ASSUMPTION (adjust if your build uses a different sentinel):
-- valid_to_date is inclusive of the last day a version was valid; the
-- current row's valid_to_date is either NULL or a high sentinel
-- (e.g. '9999-12-31'). The OR valid_to_date IS NULL branch is a no-op
-- if your build always populates a sentinel date instead of NULL.
-- =============================================================================

SET as_of_date = '2011-06-30';  -- change per run, or bind as a parameter

SELECT
    account_version_sk,
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    cdc_flag,
    valid_from_date,
    valid_to_date,
    is_current,
    _batch_id,
    _source_table
FROM brokerage_dwh.silver.silver_account
WHERE valid_from_date <= $as_of_date
  AND (valid_to_date >= $as_of_date OR valid_to_date IS NULL)
-- Safety net: guarantees exactly one row per account_id even if an
-- upstream bug ever produces overlapping valid_from/valid_to ranges.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id
    ORDER BY valid_from_date DESC
) = 1
ORDER BY account_id;