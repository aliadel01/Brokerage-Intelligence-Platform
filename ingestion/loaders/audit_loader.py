"""
Loaders for the two operational/control sources:
  - *_audit.csv       -> bronze_source_audit (vendor-supplied row counts etc.,
                                                used for reconciliation)
  - BatchDate.txt      -> bronze_batch_control (records the as-of date per batch)

load_audit_source: soft-fail, consistent with every other bronze loader.
Every audit field (dataset, date, attribute, value, dvalue) goes through
_safe_cast; failed casts yield None in the column plus a structured error
dict, packed per row into _dq_errors via _pack_dq_errors (NULL if clean).
The row always loads -- rejection/quarantine is a downstream decision.

load_batch_date: DIFFERENT policy, decided -- hard-fail, not soft-fail.
asofdate is a control-table value that downstream batch/incremental logic
depends on; a NULL asofdate landing silently (evidenced only in _dq_errors)
was judged worse here than in business tables. _safe_cast is still used to
get a structured error, but on failure the loader raises instead of writing
the row -- the batch load stops rather than registering a corrupt as-of
date. On success, _dq_errors is always NULL for this table (a row only
ever lands here when the cast succeeded).
"""

import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from ingestion.common import write_staging_csv, _safe_cast, _pack_dq_errors
from ingestion.snowflake_client import copy_into

def load_audit_source(conn, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    def _iter_rows():
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            if reader.fieldnames:
                reader.fieldnames = [name.strip() for name in reader.fieldnames if name]

            for record in reader:
                dataset, e1 = _safe_cast(record.get("DataSet"), str, "dataset")
                date, e2 = _safe_cast(record.get("Date"), lambda d: datetime.strptime(d, "%Y-%m-%d").date(), "date")
                attribute, e3 = _safe_cast(record.get("Attribute"), str, "attribute")
                value, e4 = _safe_cast(record.get("Value"), int, "value")
                dvalue, e5 = _safe_cast(record.get("DValue"), Decimal, "dvalue")

                dq_errors = _pack_dq_errors([e1, e2, e3, e4, e5])

                yield [
                    dataset,
                    date,
                    attribute,
                    value,
                    dvalue,
                    batch_id,
                    source_file,
                    loaded_at,
                    dq_errors,
                ]

    cols = ["dataset", "date", "attribute", "value", "dvalue",
        "_batch_id", "_source_file", "_loaded_at", "_dq_errors"]
    path = tmp_dir / f"audit_{filepath.stem}_b{batch_id}.csv"

    count = write_staging_csv(path, _iter_rows())
    if count == 0:
        return 0
    return copy_into(conn, "bronze_source_audit", cols, path)


def load_batch_date(conn, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    with open(filepath, "r", encoding="utf-8") as f:
        as_of_date_raw = f.read().strip()

    as_of_date, error = _safe_cast(as_of_date_raw, lambda v: datetime.strptime(v, "%Y-%m-%d").date(), "asofdate")
    
    if error:
        # Critical control value -- unlike every other loader, this one
        # hard-fails instead of landing a NULL asofdate silently. 
        raise ValueError(
            f"{filepath.name}: invalid as-of date '{as_of_date_raw}' "
            f"({error['error_type']}: {error['error_msg']})"
        )
    

    cols = ["_batch_id", "asofdate", "_loaded_at"]
    rows = [[batch_id, as_of_date, datetime.now(timezone.utc)]]
    path = tmp_dir / f"batch_control_b{batch_id}.csv"

    write_staging_csv(path, rows)
    return copy_into(conn, "bronze_batch_control", cols, path)