-- Purpose:
-- Audits access to restricted PII columns in silver_hr over the last 30 days.
-- Returns the access time, user, query ID, and SQL text for matching queries.

SELECT
    query_start_time,
    user_name,
    query_id,
    query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND EXISTS (
      SELECT 1
      FROM LATERAL FLATTEN(input => base_objects_accessed) obj,
           LATERAL FLATTEN(input => obj.value:columns) col
      WHERE UPPER(obj.value:objectName::STRING)
                = 'BROKERAGE_DWH.SILVER.SILVER_HR'
        AND LOWER(col.value:columnName::STRING) IN (
            'first_name',
            'last_name',
            'middle_initial',
            'phone'
        )
  )
ORDER BY query_start_time DESC;