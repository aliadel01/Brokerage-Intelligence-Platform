"""
CLI entry point for bronze-layer ingestion into Snowflake.

Expected directory layout:

    <data-dir>/
        batch1/
            Date.txt
            Time.txt
            StatusType.txt
            TaxRate.txt
            Industry.txt
            TradeType.txt
            HR.csv
            Prospect.csv
            CustomerMgmt.xml
            FINWIRE2015Q4          (one or more FINWIRE* files)
            Trade.txt
            TradeHistory.txt
            HoldingHistory.txt
            WatchHistory.txt
            DailyMarket.txt
            CashTransaction.txt
            BatchDate.txt
            *_audit.csv
        batch2/
            Account.txt
            Customer.txt
            Prospect.csv
            Trade.txt
            HoldingHistory.txt
            WatchHistory.txt
            DailyMarket.txt
            CashTransaction.txt
            BatchDate.txt
            *_audit.csv
        batch3/
            ...

A source that isn't present in a given batch directory is simply skipped —
scope per batch is driven entirely by file presence, so this script doesn't
need to know your batch scope matrix; the data directory expresses it.

Every loader normalizes its source into a local staging CSV, then PUTs it
to the Snowflake internal stage `ingest_stage` and COPY INTOs the target
table. Staging files are written to a temp directory that is
cleaned up at the end of each run.

Usage:
    python -m ingestion.main --batch-id 1
    python -m ingestion.main --batch-id 1 --force   # wipe + re-ingest an existing batch
    python -m ingestion.main --batch-id 1 --log-file ingest.log

Run once per batch, in order (1, then 2, then 3, ...) — _cdc_dsn versioning
assumes monotonically increasing sequence numbers across batches.
"""
import argparse
import shutil
import tempfile
from pathlib import Path
from os import getenv
from dotenv import load_dotenv

from .config import DELIMITED_SOURCES, ALL_BRONZE_TABLES
from .snowflake_client import get_connection
from .loaders.delimited_loader import load_delimited_source
from .loaders.finwire_loader import load_finwire_source
from .loaders.xml_loader import load_customer_mgmt_xml
from .loaders.audit_loader import load_audit_source, load_batch_date
from .common import get_logger

# Load environment variables early so CLI defaults can fall back to .env values cleanly.
load_dotenv()


def log_dq_event(conn, batch_id, check_type, source_file,
                  expected_value, actual_value, severity, message):
    """DQ evidence trail — one row per check result, into
    governance.dq_audit_log. Distinct from operational logging below:
    this is permanent, queryable business evidence, not progress/debug
    output."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO brokerage_dwh.governance.dq_audit_log
                    (_batch_id, check_type, source_file, expected_value,
                    actual_value, severity, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                check_type,
                source_file,
                str(expected_value) if expected_value is not None else None,
                str(actual_value) if actual_value is not None else None,
                severity,
                message
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to log DQ event: {e}") from e

    return 0


def force_delete_batch(conn, batch_id: int, log) -> None:
    """Delete all rows for this batch_id across every bronze table.

    Used only when --force is passed and the batch already exists —
    lets you wipe a partially-loaded or already-completed batch and
    re-run it cleanly from scratch. [decision: full wipe of all bronze
    tables incl. business data, not just the control tables — user's
    call, driven by storage cost, since business tables are already
    protected downstream by silver's dedup_latest/_row_hash]
    """
    with conn.cursor() as cur:
        # Wrapped in a single transaction: if a delete on one table fails
        # partway through, earlier deletes in this call roll back too,
        # instead of leaving bronze in a half-wiped, inconsistent state.
        cur.execute("BEGIN")
        try:
            for table in ALL_BRONZE_TABLES:
                cur.execute(
                    f"DELETE FROM {table} WHERE _batch_id = %s",
                    (batch_id,),
                )
                deleted_rows = cur.rowcount
                log.info(f"Deleted {deleted_rows} rows from {table}")
            cur.execute("COMMIT")
            log.info("All bronze tables cleaned up for re-ingestion.")
        except Exception:
            cur.execute("ROLLBACK")
            raise


def run_batch(conn, data_dir: Path, batch_id: int, tmp_dir: Path, log, force: bool = False) -> dict:
    """Execute bronze-layer data ingestion for a single batch directory."""

    # Capitalizing "Batch" ensures strict alignment with naming conventions on case-sensitive file systems.
    batch_dir = data_dir / f"Batch{batch_id}"
    if not batch_dir.exists():
        raise FileNotFoundError(f"No directory found for batch {batch_id}: {batch_dir}")

    # Pre-Ingestion Validation: BatchDate.txt is required to ensure idempotency
    # and proper batch tracking. Checked first, before anything else touches
    # Snowflake or the filesystem further, since every safety check below
    # (idempotency, --force) depends on this file having been loadable.
    batch_date_path = batch_dir / "BatchDate.txt"
    if not batch_date_path.exists():
        raise FileNotFoundError(f"BatchDate.txt not found for batch {batch_id}")

    # Pre-Ingestion Validation: check for existing batch records in the
    # control table before touching anything else.
    # [decision: parameterized query — not an f-string — to close the
    # SQL-injection-pattern issue raised earlier]
    query = "SELECT COUNT(*) FROM bronze_batch_control WHERE _batch_id = %s"
    with conn.cursor() as cur:
        cur.execute(query, (batch_id,))
        exists = cur.fetchone()[0] > 0

    if exists:
        if not force:
            raise RuntimeError(
                f"Batch {batch_id} has already been ingested; "
                f"aborting to prevent duplicate data. "
                f"Pass --force to delete and re-ingest it."
            )
        # [decision: --force must be explicit — default behavior stays a
        # hard refusal, so an accidental re-run of the same batch doesn't
        # silently wipe and reload data]
        log.warning("--force: deleting existing rows across all bronze tables before re-ingesting")
        force_delete_batch(conn, batch_id, log)

    summary = {}

    # [decision: bronze_batch_control loads FIRST, before anything else —
    # so that if this run fails partway through, the next run for this
    # batch_id is correctly detected as "already attempted" and routed
    # through the --force check above, rather than silently reloading
    # everything a second time.]

    # [decision: BatchDate.txt existence is now checked at the TOP of
    # run_batch(), before the idempotency check and before --force can
    # delete anything. The old behavior checked existence here instead,
    # which meant a batch with an existing control row but a missing file
    # would wipe all of that batch's bronze rows first, then raise --
    # leaving the batch empty instead of avoiding the wipe entirely.
    # Checking upfront avoids that: a missing file now blocks the whole
    # batch before any deletion or loading happens.]
    load_batch_date(conn, batch_date_path, batch_id, tmp_dir)
    log.info("BatchDate.txt -> bronze_batch_control")

    # Loaded-row tracker: stem of the source filename -> actual rows loaded.
    # Built incrementally as each source is ingested, so it can be compared
    # against bronze_source_audit's expected RowCount at the end.
    # Keys use the same stem convention as the audit file naming
    # (source_file.replace("_audit.csv", "")), so FINWIRE/XML totals (which
    # are already summed across their multiple target tables, per user's
    # confirmation) line up with the single audit row per source file.
    loaded_counts = {}

    # Dynamic Ingestion Loop: Iterate over configured sources rather than hardcoding.
    # Missing sources are skipped silently to support variable batch contents without brittle logic.
    for source_name, config in DELIMITED_SOURCES.items():
        filepath = batch_dir / config["filename"]
        if not filepath.exists():
            continue
        count = load_delimited_source(conn, config, filepath, batch_id, tmp_dir)
        summary[source_name] = count
        loaded_counts[filepath.stem] = count
        log.info(f"{source_name}: {count} rows -> {config['target_table']}")

    # Specialized Handler: XML demands custom hierarchical parsing before flattening into relational tables.
    xml_path = batch_dir / "CustomerMgmt.xml"
    if xml_path.exists():
        n_events, n_accounts = load_customer_mgmt_xml(conn, xml_path, batch_id, tmp_dir)
        summary["customer_mgmt_xml"] = n_events + n_accounts
        loaded_counts[xml_path.stem] = n_events + n_accounts
        log.info(f"CustomerMgmt.xml: {n_events} events, {n_accounts} account links")

    # File Pattern Matching: FINWIRE filenames vary by year/quarter (e.g., FINWIRE2015Q4).
    # Using glob matching decouples the script from static filename dependencies.
    finwire_files = [
        f for f in batch_dir.glob("FINWIRE*")
        if not f.name.endswith("_audit.csv")
    ]

    # Explicit Sorting: Processes historical files in deterministic alphabetical order to prevent CDC sequence race conditions.
    for finwire_path in sorted(finwire_files):
        n = load_finwire_source(conn, finwire_path, batch_id, tmp_dir)
        summary[finwire_path.name] = n
        loaded_counts[finwire_path.stem] = n
        log.info(f"{finwire_path.name}: {n} rows across CMP/SEC/FIN")

    # Reconciliation Logic: Process audit files last so row count audits can compare
    # against the freshly ingested bronze tables within the same execution scope.
    for audit_path in sorted(batch_dir.glob("*_audit.csv")):
        n = load_audit_source(conn, audit_path, batch_id, tmp_dir)
        summary[audit_path.name] = n
        log.info(f"{audit_path.name}: {n} rows -> bronze_source_audit")

    # Reconciliation check: compare each audit file's stated RowCount against
    # what actually landed in bronze, using the _source_file / stem mapping
    # built above. [decision: mismatches logged as WARNING rows in
    # governance.dq_audit_log via log_dq_event, never fatal — a mismatch
    # might have a legitimate explanation, so someone needs to notice it
    # rather than have the batch abort automatically]
    query = """
            SELECT 
                _source_file, 
                value AS total_value
            FROM 
                bronze_source_audit 
            WHERE 
                _batch_id = %s 
                AND Attribute LIKE '%%_RECORDS'

            UNION ALL

            SELECT 
                _source_file, 
                SUM(value) AS total_value
            FROM 
                bronze_source_audit
            WHERE 
                _batch_id = %s 
                AND value > 0 
                AND (
                    _source_file IN ('Account_audit.csv', 'Customer_audit.csv', 'CustomerMgmt_audit.csv')
                    OR _source_file LIKE '%%FINWIRE____Q__audit.csv%%'
                )
            GROUP BY 
                _source_file;
            """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (batch_id, batch_id),
        )
        audit_rowcounts = cur.fetchall()

    for source_file, expected in audit_rowcounts:
        stem = source_file.replace("_audit.csv", "")
        actual = loaded_counts.get(stem)
        if actual is None:
            msg = f"audit expects {stem} (RowCount={expected}) but no matching loaded source was found"
            log.warning(msg)
            log_dq_event(
                conn, batch_id, "reconciliation_mismatch", stem,
                expected, None, "WARNING", msg,
            )
        elif actual != expected:
            msg = f"row count mismatch for {stem}: audit expects {expected}, bronze has {actual}"
            log.warning(msg)
            log_dq_event(
                conn, batch_id, "reconciliation_mismatch", stem,
                expected, actual, "WARNING", msg,
            )
        else:
            # [decision: log passing checks too, not just mismatches —
            # audit artifact needs to show the check RAN and
            # what it found, not only when it failed, so an auditor can
            # see full reconciliation coverage per batch, not just
            # exceptions]
            msg = f"row count match for {stem}: {actual}"
            log_dq_event(
                conn, batch_id, "reconciliation_check", stem,
                expected, actual, "PASS", msg,
            )

    return summary


def main():
    parser = argparse.ArgumentParser(description="Bronze layer ingestion for brokerage-data-platform (Snowflake)")

    # Hybrid Configuration Pattern: Use getenv inside defaults.
    # This gives CLI parameters precedence over .env file defaults without duplicating lookup code.
    parser.add_argument("--data-dir", default=getenv("DATA_DIR"), help="Root directory containing batch1/, batch2/, ... subfolders")
    parser.add_argument("--batch-id", required=True, type=int, help="Batch number to ingest (e.g., 1, 2, 3)")
    parser.add_argument("--account", default=getenv("SNOWFLAKE_ACCOUNT"), help="Snowflake account identifier")
    parser.add_argument("--user", default=getenv("SNOWFLAKE_USER"), help="Snowflake user name")
    parser.add_argument("--password", default=getenv("SNOWFLAKE_PASSWORD"), help="Snowflake password")
    parser.add_argument("--role", default=getenv("SNOWFLAKE_ROLE"), help="Snowflake role")
    parser.add_argument("--warehouse", default=getenv("SNOWFLAKE_WAREHOUSE"), help="Snowflake warehouse")
    parser.add_argument("--database", default=getenv("SNOWFLAKE_DATABASE", "brokerage_dwh"), help="Snowflake database name")
    parser.add_argument("--schema", default=getenv("SNOWFLAKE_SCHEMA", "bronze"), help="Snowflake schema name")

    # Optional Force Flag: Allows users to explicitly delete existing batch data for re-ingestion.
    # [decision: opt-in flag rather than automatic delete-then-insert, so
    # that re-running the same batch by accident still fails loudly]
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing rows for this batch across all bronze tables before re-ingesting."
    )

    # Operational log file: separate from governance.dq_audit_log — this is
    # plain progress/error output, optionally mirrored to a file.
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional path to also write operational logs to a file."
    )

    args = parser.parse_args()

    # Fail-Fast Strategy: Validate all essential parameters early before initializing heavy resources like database drivers.
    required_configs = {
        "DATA_DIR": args.data_dir,
        "SNOWFLAKE_ACCOUNT": args.account,
        "SNOWFLAKE_USER": args.user,
        "SNOWFLAKE_PASSWORD": args.password,
        "SNOWFLAKE_ROLE": args.role,
        "SNOWFLAKE_WAREHOUSE": args.warehouse,
    }

    missing_keys = [key for key, val in required_configs.items() if not val]
    if missing_keys:
        parser.error(f"Missing configuration for: {', '.join(missing_keys)}. Please set them in your .env file or pass them via CLI flags.")

    log = get_logger(args.batch_id, log_file=args.log_file)

    conn = get_connection(args)
    data_dir = Path(args.data_dir)

    # Isolated Temp Directory: Creates a unique runtime directory for intermediate CSV staging,
    # preventing concurrency collisions if multiple ingestion jobs execute in parallel.
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"bronze_ingest_batch{args.batch_id}_"))

    # Cleanup Guarantee: Wrapping logic in try...finally guarantees that DB connections
    # are closed and temporary local disk storage is deleted, even if exceptions occur mid-batch.
    try:
        summary = run_batch(conn, data_dir, args.batch_id, tmp_dir, log, force=args.force)
        total = sum(v for v in summary.values() if isinstance(v, int))
        log.info(f"Batch {args.batch_id} complete. Total rows ingested: {total}")
    finally:
        conn.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()