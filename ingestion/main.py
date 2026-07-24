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

Run once per batch, in order (1, then 2, then 3, ...) — _cdc_dsn versioning
assumes monotonically increasing sequence numbers across batches.
"""
import argparse
import shutil
import tempfile
from pathlib import Path
from os import getenv
from dotenv import load_dotenv

from .config import DELIMITED_SOURCES
from .snowflake_client import get_connection
from .loaders.delimited_loader import load_delimited_source
from .loaders.finwire_loader import load_finwire_source
from .loaders.xml_loader import load_customer_mgmt_xml
from .loaders.audit_loader import load_audit_source, load_batch_date

# Load environment variables early so CLI defaults can fall back to .env values cleanly.
load_dotenv()

def run_batch(conn, data_dir: Path, batch_id: int, tmp_dir: Path) -> dict:
    """Execute bronze-layer data ingestion for a single batch directory."""
    
    # Capitalizing "Batch" ensures strict alignment with naming conventions on case-sensitive file systems.
    batch_dir = data_dir / f"Batch{batch_id}"
    if not batch_dir.exists():
        raise FileNotFoundError(f"No directory found for batch {batch_id}: {batch_dir}")

    summary = {}

    # Sequential Dependency: Process control metadata first so Snowflake target tables 
    # receive the batch context before primary payload rows are ingested.
    batch_date_path = batch_dir / "BatchDate.txt"
    if batch_date_path.exists():
        load_batch_date(conn, batch_date_path, batch_id, tmp_dir)
        print(f"[batch {batch_id}] BatchDate.txt -> bronze_batch_control")

    # Dynamic Ingestion Loop: Iterate over configured sources rather than hardcoding.
    # Missing sources are skipped silently to support variable batch contents without brittle logic.
    for source_name, config in DELIMITED_SOURCES.items():
        filepath = batch_dir / config["filename"]
        if not filepath.exists():
            continue
        count = load_delimited_source(conn, config, filepath, batch_id, tmp_dir)
        summary[source_name] = count
        print(f"[batch {batch_id}] {source_name}: {count} rows -> {config['target_table']}")

    # Specialized Handler: XML demands custom hierarchical parsing before flattening into relational tables.
    xml_path = batch_dir / "CustomerMgmt.xml"
    if xml_path.exists():
        n_events, n_accounts = load_customer_mgmt_xml(conn, xml_path, batch_id, tmp_dir)
        summary["customer_mgmt_xml"] = n_events + n_accounts
        print(f"[batch {batch_id}] CustomerMgmt.xml: {n_events} events, {n_accounts} account links")
        
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
        print(f"[batch {batch_id}] {finwire_path.name}: {n} rows across CMP/SEC/FIN")

    # Reconciliation Logic: Process audit files last so row count audits can compare 
    # against the freshly ingested bronze tables within the same execution scope.
    for audit_path in sorted(batch_dir.glob("*_audit.csv")):
        n = load_audit_source(conn, audit_path, batch_id, tmp_dir)
        summary[audit_path.name] = n
        print(f"[batch {batch_id}] {audit_path.name}: {n} rows -> bronze_source_audit")

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

    conn = get_connection(args)
    
    # Isolated Temp Directory: Creates a unique runtime directory for intermediate CSV staging,
    # preventing concurrency collisions if multiple ingestion jobs execute in parallel.
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"bronze_ingest_batch{args.batch_id}_"))

    # Cleanup Guarantee: Wrapping logic in try...finally guarantees that DB connections 
    # are closed and temporary local disk storage is deleted, even if exceptions occur mid-batch.
    try:
        summary = run_batch(conn, data_dir, args.batch_id, tmp_dir)
        total = sum(v for v in summary.values() if isinstance(v, int))
        print(f"\nBatch {args.batch_id} complete. Total rows ingested: {total}")
    finally:
        conn.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()