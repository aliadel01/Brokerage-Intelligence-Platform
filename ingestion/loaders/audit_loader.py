"""
Loaders for the two operational/control sources:
  - *_audit.csv       -> bronze_source_audit (vendor-supplied row counts etc.,
                                                used for reconciliation)
  - BatchDate.txt      -> bronze_batch_control (records the as-of date per batch)
DQ: There are Silent Failures but I will solve them in DQ phase.
"""
import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from ingestion.common import write_staging_csv, _safe_cast
from ingestion.snowflake_client import copy_into

def load_audit_source(conn, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    """
    Reads vendor-supplied audit CSV records into memory, normalizes types, 
    appends ingestion lineage metadata, and bulk loads them into Snowflake.
    """
    source_file = filepath.name
    # Audit Lineage Standard: Standardize timestamp format using UTC to maintain consistent operational metadata across runs.
    loaded_at = datetime.now(timezone.utc)

    def _iter_rows():
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            # Defensive Header Cleaning: Vendors frequently leave trailing spaces in column headers (e.g., "DataSet ").
            # Stripping header strings upfront prevents runtime KeyError exceptions when evaluating DictReader keys.
            if reader.fieldnames:
                reader.fieldnames = [name.strip() for name in reader.fieldnames if name]

            # Lazy Memory Generator: Yields parsed records row-by-row instead of allocating a full list in memory.
            # Integrates system metadata columns (_batch_id, _source_file, _loaded_at) directly during iteration.
            for record in reader:
                yield [
                    _safe_cast(record.get("DataSet"), str),
                    _safe_cast(record.get("BatchID"), int),
                    _safe_cast(record.get("Date"), lambda d: datetime.strptime(d, "%Y-%m-%d").date()),
                    _safe_cast(record.get("Attribute"), str),
                    _safe_cast(record.get("Value"), int),
                    _safe_cast(record.get("DValue"), Decimal),
                    batch_id,
                    source_file,
                    loaded_at,
                ]

    # Explicit Target Columns Mapping: Defines exact column ordering matching the target table DDL to prevent COPY INTO positioning errors.
    cols = ["DataSet", "BatchID", "Date", "Attribute", "Value", "DValue",
            "_batch_id", "_source_file", "_loaded_at"]
    path = tmp_dir / f"audit_{filepath.stem}_b{batch_id}.csv"
    
    # Execution Guard: Checks row count before invoking database operations to save unnecessary Snowflake staging/PUT queries for empty audit files.
    count = write_staging_csv(path, _iter_rows())
    if count == 0:
        return 0
    return copy_into(conn, "bronze_source_audit", cols, path)


def load_batch_date(conn, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    """
    Reads the operational as-of date from BatchDate.txt and registers it in 'bronze_batch_control'.
    """
    # Single-Value Extraction: Reads and strips whitespace from the small control file directly.
    with open(filepath, "r", encoding="utf-8") as f:
        as_of_date_raw = f.read().strip()
    
    as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()

    # Control Table Standard: Isolates batch processing context metadata to drive downstream Silver layer incremental transformations.
    cols = ["BatchID", "AsOfDate", "_loaded_at"]
    rows = [[batch_id, as_of_date, datetime.now(timezone.utc)]]
    path = tmp_dir / f"batch_control_b{batch_id}.csv"
    
    write_staging_csv(path, rows)
    return copy_into(conn, "bronze_batch_control", cols, path)