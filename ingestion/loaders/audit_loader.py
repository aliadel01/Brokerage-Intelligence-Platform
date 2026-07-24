"""
Loaders for the two operational/control sources:
  - *_audit.csv       -> bronze_source_audit (vendor-supplied row counts etc.,
                          used for reconciliation — see ADR-005)
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
        1. Reads vendor-supplied audit CSV records into memory.
        2. Normalizes data types (dates, integers, decimals) and appends ingestion metadata (_batch_id, _source_file, _loaded_at).
        3. Writes the processed rows into a temporary staging CSV file.
        4. Bulk loads the staged CSV into 'bronze_source_audit' via Snowflake COPY INTO.
    """
    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    def _iter_rows():
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            # Trim whitespace from header names because the actual CSV files have trailing spaces in the header row.
            if reader.fieldnames:
                reader.fieldnames = [name.strip() for name in reader.fieldnames if name]

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

    cols = ["DataSet", "BatchID", "Date", "Attribute", "Value", "DValue",
            "_batch_id", "_source_file", "_loaded_at"]
    path = tmp_dir / f"audit_{filepath.stem}_b{batch_id}.csv"
    count = write_staging_csv(path, _iter_rows())
    if count == 0:
        return 0
    return copy_into(conn, "bronze_source_audit", cols, path)


def load_batch_date(conn, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    """
    Reads the as-of date from BatchDate.txt, normalizes it, and loads it into 'bronze_batch_control'.
    Operational Steps:
        1. Read the single line from BatchDate.txt and parse it as a date.
        2. Create a single-row record with BatchID, AsOfDate, and _loaded_at.
        3. Write the record to a temporary staging CSV file.
        4. Bulk load the staged CSV into 'bronze_batch_control' via Snowflake COPY INTO.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        as_of_date_raw = f.read().strip()
    as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()

    cols = ["BatchID", "AsOfDate", "_loaded_at"]
    rows = [[batch_id, as_of_date, datetime.now(timezone.utc)]]
    path = tmp_dir / f"batch_control_b{batch_id}.csv"
    write_staging_csv(path, rows)
    return copy_into(conn, "bronze_batch_control", cols, path)